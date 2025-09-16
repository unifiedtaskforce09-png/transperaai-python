import os
import time
import json
import tempfile
import logging
from typing import List, Optional, Dict
from dotenv import load_dotenv
load_dotenv()

# Basic logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Use google.generativeai SDK
try:
    import google.generativeai as genai
    from google.generativeai import types as genai_types
    
    logger.info("Successfully imported google.generativeai SDK.")
except ImportError:
    genai = None
    genai_types = None
    logger.error("Google GenAI SDK not found. Install with: pip install -U google-generativeai")

class GeminiClient:
    """
    A client for interacting with the Google Gemini API, with a focus on
    batch processing and a reliable sequential fallback.
    """
    def __init__(self, api_key: Optional[str] = None, max_retries: int = 3):
        """Initialize Gemini client with API key from param or environment."""
        if genai is None:
            raise ImportError("Google GenAI SDK not found. Please install it to use this client.")

        self.api_key = api_key or os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
        if not self.api_key:
            raise ValueError("No API key provided. Set either GEMINI_API_KEY or GOOGLE_API_KEY environment variable.")
        
        # Configure the SDK
        genai.configure(api_key=self.api_key)
        
        self.max_retries = max_retries
        # FIX 1: Use a valid, publicly available model name.
        self.model = "gemini-2.5-flash-lite"
        logger.info(f"GeminiClient initialized with model: {self.model}")
        
    def set_model(self, model_name: str):
        """Change the model being used."""
        self.model = model_name
        logger.info(f"Model changed to: {self.model}")

    def submit_async_batch(self, payloads: List[str], model: Optional[str] = None, display_name: Optional[str] = None) -> List[str]:
        """Process a batch of prompts using the current SDK's batch capabilities."""
        if not payloads:
            raise ValueError("No payloads provided for batch submission.")
        
        model_name = model or self.model
        model = genai.GenerativeModel(model_name)
        
        # Prepare the requests
        try:
            # Create a list of generation tasks
            tasks = []
            for payload in payloads:
                tasks.append(
                    model.generate_content(
                        {"text": payload},
                        generation_config=genai.types.GenerationConfig(
                            temperature=0.3  # Keep temperature low for translations
                        )
                    )
                )
            
            # Run all tasks
            responses = []
            for task in tasks:
                try:
                    response = task
                    responses.append(response)
                except Exception as e:
                    logger.warning(f"Task failed: {e}")
                    responses.append(None)
            
            # Extract results
            results = []
            for response in responses:
                if response is None:
                    results.append("")
                else:
                    try:
                        results.append(response.text)
                    except (AttributeError, ValueError) as e:
                        logger.warning(f"Failed to extract text from response: {e}")
                        results.append("")
            
            return results
            
        except Exception as e:
            logger.error(f"Batch processing failed: {e}")
            raise

        # FIX 3: Parse results in the order they appear, as the API guarantees it.
        results: List[str] = []
        for line in file_content.splitlines():
            if not line.strip():
                continue
            try:
                obj = json.loads(line)
                response = obj.get("response", {})
                candidates = response.get("candidates", [])
                if candidates and candidates[0].get("content"):
                    parts = candidates[0]["content"].get("parts", [])
                    if parts:
                        results.append(parts[0].get("text", ""))
            except (json.JSONDecodeError, KeyError, IndexError) as e:
                logger.warning(f"Failed to parse a result line from batch output: {e}")
                results.append("") # Append empty string for failed parses to maintain order
        
        return results

    def process_batch(self, prompts: List[str], **kwargs) -> List[str]:
        """
        Process a batch of prompts using the current SDK's batch capabilities.
        Falls back to sequential processing if batch mode fails.
        """
        if isinstance(prompts, str):
            prompts = [prompts]
        if not prompts:
            return []
        
        try:
            return self.submit_async_batch(prompts)
        except Exception as e:
            logger.warning(f"Batch mode failed: {e}. Falling back to sequential processing.")
            return self.process_sequential(prompts, **kwargs)

    def process_sequential(self, prompts: List[str], system_prompt: Optional[str] = None,
                           temperature: float = 0.5, max_output_tokens: int = 4096) -> List[str]:
        """Process prompts one by one as a fallback, with retries."""
        results = []
        for i, prompt in enumerate(prompts):
            full_prompt = f"{system_prompt}\n\n{prompt}" if system_prompt else prompt
            
            retry_count = 0
            while retry_count <= self.max_retries:
                try:
                    result = self._call_gemini_with_sdk(
                        full_prompt,
                        model=self.model,
                        temperature=temperature,
                        max_output_tokens=max_output_tokens
                    )
                    results.append(result)
                    break
                except Exception as e:
                    retry_count += 1
                    if retry_count > self.max_retries:
                        logger.error(f"Request {i+1} failed after {self.max_retries} retries: {e}")
                        results.append("") # Add empty string for failed requests
                        break
                    
                    wait_time = min(2 ** retry_count, 30) # Exponential backoff
                    logger.warning(f"Request {i+1} attempt {retry_count} failed. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
        return results

    def _call_gemini_with_sdk(self, prompt_text: str, model: str, temperature: float, max_output_tokens: int) -> str:
        """Helper to make a single SDK call with proper configuration."""
        model_obj = genai.GenerativeModel(model)
        
        # FIX 4: Correctly apply generation parameters via GenerationConfig.
        generation_config = genai.types.GenerationConfig(
            temperature=temperature,
            max_output_tokens=max_output_tokens
        )
        
        response = model_obj.generate_content(
            prompt_text,
            generation_config=generation_config
        )
        
        try:
            return response.text
        except ValueError:
            # Handle cases where the response might be blocked
            logger.warning(f"Response for a prompt was blocked or empty. Full response: {response.prompt_feedback}")
            return ""