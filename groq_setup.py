"""
Groq API client implementation with batch processing support.
Provides optimized handling for batch translation requests.
"""

import os
import time
import logging
from typing import List, Dict, Any, Optional

# Optional Groq SDK
try:
    from groq import Groq
except Exception:
    Groq = None

logger = logging.getLogger(__name__)

class GroqClient:
    def __init__(self, api_key=None, max_retries=3):
        """Initialize Groq client with API key from param or environment"""
        self.api_key = api_key or os.environ.get("GROQ_API_KEY") or os.environ.get("GROQ_KEY")
        if not self.api_key:
            raise ValueError("No Groq API key provided in parameters or environment variables")
        
        if Groq is None:
            raise RuntimeError("Groq SDK is not installed. Install with: pip install -U groq")
            
        self.client = Groq(api_key=self.api_key)
        self.max_retries = max_retries
        self.model = "llama-3.1-70b-versatile"  # Default model
        
    def set_model(self, model_name: str):
        """Change the model being used"""
        self.model = model_name
        
    def _call_groq_optimized(self, messages, temperature=0.3, max_tokens=None):
        """
        Optimized Groq API call with retries and error handling.
        Similar to the implementation in advanced_docx_translator.py.
        """
        if Groq is None:
            raise RuntimeError("Groq SDK is not installed or available. Install the Groq SDK or switch engine.")
            
        retry_count = 0
        while retry_count <= self.max_retries:
            try:
                # Create params dictionary
                params = {"messages": messages, "model": self.model, "temperature": temperature}
                if max_tokens is not None:
                    params["max_tokens"] = max_tokens

                # Make API call
                response = self.client.chat.completions.create(**params)

                # Return the response content
                return response.choices[0].message.content.strip()
                
            except Exception as e:
                retry_count += 1
                error_str = str(e).lower()
                is_rate_limit = '429' in error_str or 'rate limit' in error_str or 'quota' in error_str
                
                if retry_count > self.max_retries:
                    logger.error(f"Groq request failed after {self.max_retries} attempts: {e}")
                    raise  # Re-raise the exception
                
                # Exponential backoff, longer for rate limit errors
                wait_time = (3 ** retry_count) if is_rate_limit else (2 ** retry_count)
                wait_time = min(wait_time, 20)  # Cap wait time at 20 seconds
                logger.warning(f"Groq API attempt {retry_count} failed: {e}, retrying in {wait_time}s...")
                time.sleep(wait_time)
                
        # This should never be reached due to the exception above
        raise RuntimeError("Failed to get response from Groq API")
        
    def process_batch(self, 
                     prompts: List[str], 
                     system_prompt: str = None,
                     temperature: float = 0.2,
                     max_tokens: Optional[int] = None) -> List[str]:
        """
        Process a batch of prompts using Groq API.
        
        Note: Unlike Gemini, Groq doesn't have a native batch API.
        This implementation does smart batching by combining prompts
        when possible or processing sequentially with proper rate limiting.
        
        Args:
            prompts: List of text prompts to process
            system_prompt: Optional system prompt to apply to all requests
            temperature: Sampling temperature
            max_tokens: Maximum tokens to generate (optional)
        
        Returns:
            List of generated responses in same order as input prompts
        """
        if not prompts:
            return []
        
        # Determine batching strategy based on prompt count and structure
        if len(prompts) == 1:
            # Single prompt - direct request with system prompt
            try:
                messages = []
                if system_prompt:
                    messages.append({"role": "system", "content": system_prompt})
                messages.append({"role": "user", "content": prompts[0]})
                
                response = self._call_groq_optimized(
                    messages=messages,
                    temperature=temperature,
                    max_tokens=max_tokens
                )
                return [response]
            except Exception as e:
                logger.error(f"Error in single Groq request: {e}")
                return [""]  # Empty string as fallback
        
        # For multiple prompts with a system prompt, use a special format
        # that includes indices to keep track of individual responses
        if len(prompts) <= 10 and system_prompt:
            try:
                # Format batch with markers for Groq
                combined_prompt = "\n".join(f'<ITEM idx="{i}">{prompt}</ITEM>' for i, prompt in enumerate(prompts))
                
                # Send as one request with system prompt
                messages = [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": combined_prompt}
                ]
                
                # Process batch
                response = self._call_groq_optimized(
                    messages=messages,
                    temperature=temperature,
                    max_tokens=max_tokens
                )
                
                # Extract indexed responses
                import re
                item_pattern = re.compile(r'<\s*ITEM[^>]*\bidx\s*=\s*["\']?(\d+)["\']?[^>]*>(.*?)</\s*ITEM\s*>', re.DOTALL | re.IGNORECASE)
                found = item_pattern.findall(response)
                
                # Reconstruct results array
                results = [""] * len(prompts)  # Initialize with empty strings
                if found:
                    for idx_str, content in found:
                        try:
                            idx = int(idx_str)
                            if 0 <= idx < len(prompts):
                                results[idx] = content.strip()
                        except (ValueError, IndexError):
                            continue
                    
                    # Check if we have any missing responses and process them individually
                    missing_indices = [i for i, r in enumerate(results) if not r]
                    if missing_indices:
                        logger.warning(f"Missing responses for indices {missing_indices}, processing individually")
                        for idx in missing_indices:
                            individual_messages = []
                            if system_prompt:
                                individual_messages.append({"role": "system", "content": system_prompt})
                            individual_messages.append({"role": "user", "content": prompts[idx]})
                            
                            try:
                                time.sleep(1.0)  # Rate limiting
                                indiv_response = self._call_groq_optimized(
                                    messages=individual_messages,
                                    temperature=temperature,
                                    max_tokens=max_tokens
                                )
                                results[idx] = indiv_response
                            except Exception as e:
                                logger.error(f"Individual request for index {idx} failed: {e}")
                                # Keep as empty string
                    
                    return results
                else:
                    # If parsing fails, fall back to sequential processing
                    logger.warning("Failed to parse batch response with markers, falling back to sequential")
            except Exception as e:
                logger.error(f"Batch processing failed: {e}, falling back to sequential")
        
        # Sequential processing for larger batches or when batch processing fails
        return self.process_sequential(prompts, system_prompt, temperature, max_tokens)
        
    def process_sequential(self, prompts: List[str], system_prompt: str = None,
                          temperature: float = 0.2, max_tokens: Optional[int] = None) -> List[str]:
        """Process prompts sequentially with proper rate limiting"""
        results = []
        for i, prompt in enumerate(prompts):
            # Add rate limiting between requests
            if i > 0:
                time.sleep(0.5)  # Simple rate limiting
            
            # Prepare messages for this prompt
            messages = []
            if system_prompt:
                messages.append({"role": "system", "content": system_prompt})
            messages.append({"role": "user", "content": prompt})
            
            try:
                logger.debug(f"Sending sequential request {i+1}/{len(prompts)} to Groq API")
                response = self._call_groq_optimized(
                    messages=messages,
                    temperature=temperature,
                    max_tokens=max_tokens
                )
                results.append(response)
            except Exception as e:
                logger.error(f"Sequential request {i+1} failed: {e}")
                results.append("")  # Empty string as fallback
        
        return results
