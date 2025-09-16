from typing import List, Dict, Any, Optional
import os
import re
import time
import logging
import shutil
import json
import threading
import hashlib
from html import unescape
from io import BytesIO

from docx import Document
from docx.oxml import OxmlElement
from docx.oxml.ns import qn, nsmap

# import RTL helpers
from rtl_utils import is_rtl_language, is_arabic_text, _set_xml_para_rtl, ensure_paragraph_line_spacing, ensure_textnode_preserve_space

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Optional external client classes (lazy-resolved)
GroqClient = None
GeminiClient = None
try:
    from groq_setup import GroqClient as _GroqClient  # type: ignore
    GroqClient = _GroqClient
except Exception:
    GroqClient = None

try:
    from gemini_setup import GeminiClient as _GeminiClient  # type: ignore
    GeminiClient = _GeminiClient
except Exception:
    GeminiClient = None

# Client instances (lazy)
groq_client = None
gemini_client = None

# ------------------------------
# Namespaces & small globals (kept here for other xpath usages)
# ------------------------------
_WPS = 'http://schemas.microsoft.com/office/word/2010/wordprocessingShape'
_VML = 'urn:schemas-microsoft-com:vml'
_MATH_NS = 'http://schemas.openxmlformats.org/officeDocument/2006/math'
_NS = {'w': nsmap['w'], 'wps': _WPS, 'v': _VML, 'm': _MATH_NS}

XML_SPACE = '{http://www.w3.org/XML/1998/namespace}space'

# Simple cache (thread-safe)
translation_cache: Dict[str, Dict[str, str]] = {}
cache_max_size = 5000
_cache_lock = threading.Lock()

# Rate limiter
_last_request_time = 0.0
_min_request_interval = 0.25
_rate_lock = threading.Lock()

# Token protection patterns
_TOKEN_PATTERNS = [
    # URLs and links
    (re.compile(r'https?://\S+'), 'URL'),
    (re.compile(r'www\.\S+\.\w{2,}'), 'URL'),
    
    # Email addresses
    (re.compile(r'\b[\w\.-]+@[\w\.-]+\.\w{2,}\b'), 'EMAIL'),
    
    # Numbers with units (expanded unit list)
    (re.compile(r'\b\d{1,3}(?:[,\d]{0,})?(?:\.\d+)?\s*(?:kg|g|mg|lbs|oz|m|cm|mm|km|mi|ft|in|%|°C|°F|K|yr|yrs|s|ms|Hz|rpm|V|A|W|kW|MB|GB|TB|Mbps|Gbps)?\b', re.IGNORECASE), 'NUMUNIT'),
    
    # Dates in various formats
    (re.compile(r'\b\d{4}-\d{2}-\d{2}\b'), 'DATE'),
    (re.compile(r'\b\d{1,2}/\d{1,2}/\d{2,4}\b'), 'DATE'),
    (re.compile(r'\b\d{1,2}-\d{1,2}-\d{2,4}\b'), 'DATE'),
    
    # Removed acronym/abbreviation protection to allow full translation
    
    # Version numbers
    (re.compile(r'\b\d+\.\d+(?:\.\d+)*(?:-[a-zA-Z0-9]+)?\b'), 'VERSION'),
    
    # File extensions
    (re.compile(r'\.[A-Za-z]{2,4}\b'), 'EXTENSION'),
    
    # Currency
    (re.compile(r'(?:USD|EUR|GBP|INR|JPY|CNY|₹|$|€|£|¥)\s*\d+(?:,\d{3})*(?:\.\d{2})?\b'), 'CURRENCY'),
    
    # Phone numbers
    (re.compile(r'\+?\d{1,3}[-\s]?\d{3,4}[-\s]?\d{4}\b'), 'PHONE'),
]

# ------------------------------
# Cache functions
# ------------------------------
def get_cached_translation(key: str) -> Optional[Dict[str, str]]:
    with _cache_lock:
        return translation_cache.get(key)

def set_cached_translation(key: str, value: Dict[str, str]) -> None:
    with _cache_lock:
        if len(translation_cache) >= cache_max_size:
            translation_cache.pop(next(iter(translation_cache)))
        translation_cache[key] = value

# ------------------------------
# Rate limiting
# ------------------------------
def rate_limit_wait() -> None:
    global _last_request_time
    with _rate_lock:
        elapsed = time.time() - _last_request_time
        if elapsed < _min_request_interval:
            wait_time = _min_request_interval - elapsed
            logger.debug(f"Rate limiting: waiting {wait_time:.2f}s")
            time.sleep(wait_time)
        _last_request_time = time.time()

# ------------------------------
# Token protection
# ------------------------------
def protect_tokens(text: str, skip_tags: Optional[set] = None) -> tuple[str, Dict[str, str]]:
    """Protect special tokens from translation by replacing them with placeholders.
    skip_tags: optional set of tag names to skip protection for (e.g., {'ACRONYM'}).
    """
    if not text:
        return text, {}
        
    placeholders: Dict[str, str] = {}
    counter = 0
    protected = text

    def repl(m, tag):
        nonlocal counter
        token = m.group(0)
        # Keep very short tokens (1-2 chars) as is
        if len(token) <= 2 and not any(c.isdigit() for c in token):
            return token
            
        key = f"<{tag}_{counter}>"  # More translation-friendly format
        placeholders[key] = token
        counter += 1
        return key

    # First protect exact matches that should never be translated
    # Removed ABBR-related exact matches so they can be translated
    exact_matches = {
        'Mr.': 'TITLE',
        'Mrs.': 'TITLE',
        'Dr.': 'TITLE',
        'Prof.': 'TITLE',
        'Sr.': 'TITLE',
        'Jr.': 'TITLE',
    }
    
    for exact, tag in exact_matches.items():
        # Respect skip_tags for exact matches as well
        if skip_tags and tag in skip_tags:
            continue
        if exact in protected:
            key = f"<{tag}_{counter}>"
            placeholders[key] = exact
            protected = protected.replace(exact, key)
            counter += 1

    # Then apply regex patterns
    for pattern, tag in _TOKEN_PATTERNS:
        if skip_tags and tag in skip_tags:
            continue
        protected = pattern.sub(lambda m, t=tag: repl(m, t), protected)
        
    return protected, placeholders

def restore_tokens(text: str, placeholders: Dict[str, str]) -> str:
    if not text or not placeholders:
        return text
    # replace longer placeholder keys first
    for k in sorted(placeholders.keys(), key=len, reverse=True):
        text = text.replace(k, placeholders[k])
    return text

# ------------------------------
# Numeric localization helpers
# ------------------------------
def localize_digits(text: str, target_language: Optional[str]) -> str:
    if not text or not target_language:
        return text
    lang = str(target_language).lower()
    try:
        if lang.startswith('hi'):
            # Devanagari digits
            mapping = str.maketrans('0123456789', '०१२३४५६७८९')
            return text.translate(mapping)
        elif lang.startswith('ar') and not lang.startswith('ar-'):  # generic Arabic
            mapping = str.maketrans('0123456789', '٠١٢٣٤٥٦٧٨٩')
            return text.translate(mapping)
    except Exception:
        pass
    return text

def localize_digits_outside_placeholders(text: str, target_language: Optional[str]) -> str:
    if not text:
        return text
    # Placeholders are of the form <TAG_n>
    pattern = re.compile(r'(<[A-Z]+_\d+>)')
    try:
        segments = pattern.split(text)
        for i, seg in enumerate(segments):
            if not pattern.fullmatch(seg):
                segments[i] = localize_digits(seg, target_language)
        return ''.join(segments)
    except Exception:
        return localize_digits(text, target_language)

# ------------------------------
# XML helpers and formatting preservation
# ------------------------------

def get_run_formatting(r_elem) -> Dict[str, Any]:
    """Extract all formatting attributes from a run element."""
    formatting = {}
    try:
        rPr = r_elem.find(qn('w:rPr'))
        if rPr is not None:
            # Text color
            color = rPr.find(qn('w:color'))
            if color is not None:
                formatting['color'] = color.get(qn('w:val'))
            
            # Highlight color
            highlight = rPr.find(qn('w:highlight'))
            if highlight is not None:
                formatting['highlight'] = highlight.get(qn('w:val'))
            
            # Bold
            bold = rPr.find(qn('w:b'))
            if bold is not None:
                formatting['bold'] = bold.get(qn('w:val'), '1') != '0'
            
            # Italic
            italic = rPr.find(qn('w:i'))
            if italic is not None:
                formatting['italic'] = italic.get(qn('w:val'), '1') != '0'
            
            # Underline
            underline = rPr.find(qn('w:u'))
            if underline is not None:
                formatting['underline'] = underline.get(qn('w:val'))
            
            # Strike-through
            strike = rPr.find(qn('w:strike'))
            if strike is not None:
                formatting['strike'] = strike.get(qn('w:val'), '1') != '0'
            
            # Font size
            sz = rPr.find(qn('w:sz'))
            if sz is not None:
                try:
                    formatting['size'] = int(sz.get(qn('w:val')))
                except (ValueError, TypeError):
                    pass
            
            # Font name
            rFonts = rPr.find(qn('w:rFonts'))
            if rFonts is not None:
                formatting['font'] = (
                    rFonts.get(qn('w:ascii')) or 
                    rFonts.get(qn('w:hAnsi')) or 
                    rFonts.get(qn('w:cs'))
                )
            
            # Vertical alignment (superscript/subscript)
            vert_align = rPr.find(qn('w:vertAlign'))
            if vert_align is not None:
                formatting['vert_align'] = vert_align.get(qn('w:val'))
    except Exception as e:
        logger.debug(f"Error extracting run formatting: {e}")
    
    return formatting

def apply_run_formatting(r_elem, formatting: Dict[str, Any]) -> None:
    """Apply formatting attributes to a run element."""
    try:
        rPr = r_elem.find(qn('w:rPr'))
        if rPr is None:
            rPr = OxmlElement('w:rPr')
            r_elem.insert(0, rPr)
        
        # Text color
        if 'color' in formatting:
            color = rPr.find(qn('w:color'))
            if color is None:
                color = OxmlElement('w:color')
                rPr.append(color)
            color.set(qn('w:val'), formatting['color'])
        
        # Highlight color
        if 'highlight' in formatting:
            highlight = rPr.find(qn('w:highlight'))
            if highlight is None:
                highlight = OxmlElement('w:highlight')
                rPr.append(highlight)
            highlight.set(qn('w:val'), formatting['highlight'])
        
        # Bold
        if 'bold' in formatting:
            bold = rPr.find(qn('w:b'))
            if bold is None:
                bold = OxmlElement('w:b')
                rPr.append(bold)
            if not formatting['bold']:
                bold.set(qn('w:val'), '0')
        
        # Italic
        if 'italic' in formatting:
            italic = rPr.find(qn('w:i'))
            if italic is None:
                italic = OxmlElement('w:i')
                rPr.append(italic)
            if not formatting['italic']:
                italic.set(qn('w:val'), '0')
        
        # Underline
        if 'underline' in formatting:
            underline = rPr.find(qn('w:u'))
            if underline is None:
                underline = OxmlElement('w:u')
                rPr.append(underline)
            underline.set(qn('w:val'), formatting['underline'])
        
        # Strike-through
        if 'strike' in formatting:
            strike = rPr.find(qn('w:strike'))
            if strike is None:
                strike = OxmlElement('w:strike')
                rPr.append(strike)
            if not formatting['strike']:
                strike.set(qn('w:val'), '0')
        
        # Font size
        if 'size' in formatting:
            sz = rPr.find(qn('w:sz'))
            if sz is None:
                sz = OxmlElement('w:sz')
                rPr.append(sz)
            sz.set(qn('w:val'), str(formatting['size']))
            
            # Also set complex script size
            szCs = rPr.find(qn('w:szCs'))
            if szCs is None:
                szCs = OxmlElement('w:szCs')
                rPr.append(szCs)
            szCs.set(qn('w:val'), str(formatting['size']))
        
        # Font name (only if not a heading)
        if 'font' in formatting:
            rFonts = rPr.find(qn('w:rFonts'))
            if rFonts is None:
                rFonts = OxmlElement('w:rFonts')
                rPr.append(rFonts)
            rFonts.set(qn('w:ascii'), formatting['font'])
            rFonts.set(qn('w:hAnsi'), formatting['font'])
            rFonts.set(qn('w:cs'), formatting['font'])
        
        # Vertical alignment
        if 'vert_align' in formatting:
            vert_align = rPr.find(qn('w:vertAlign'))
            if vert_align is None:
                vert_align = OxmlElement('w:vertAlign')
                rPr.append(vert_align)
            vert_align.set(qn('w:val'), formatting['vert_align'])
            
    except Exception as e:
        logger.debug(f"Error applying run formatting: {e}")

def get_paragraph_formatting(p_elem) -> Dict[str, Any]:
    """Extract paragraph-level formatting."""
    formatting = {}
    try:
        pPr = p_elem.find(qn('w:pPr'))
        if pPr is not None:
            # Alignment
            jc = pPr.find(qn('w:jc'))
            if jc is not None:
                formatting['alignment'] = jc.get(qn('w:val'))
            
            # Indentation
            ind = pPr.find(qn('w:ind'))
            if ind is not None:
                formatting['indent_left'] = ind.get(qn('w:left'))
                formatting['indent_right'] = ind.get(qn('w:right'))
                formatting['indent_first'] = ind.get(qn('w:firstLine'))
                formatting['hanging'] = ind.get(qn('w:hanging'))
            
            # Spacing
            spacing = pPr.find(qn('w:spacing'))
            if spacing is not None:
                formatting['space_before'] = spacing.get(qn('w:before'))
                formatting['space_after'] = spacing.get(qn('w:after'))
                formatting['line_spacing'] = spacing.get(qn('w:line'))
                formatting['line_rule'] = spacing.get(qn('w:lineRule'))
            
            # Borders
            pBdr = pPr.find(qn('w:pBdr'))
            if pBdr is not None:
                for border in ['top', 'left', 'bottom', 'right']:
                    b = pBdr.find(qn(f'w:{border}'))
                    if b is not None:
                        formatting[f'border_{border}'] = {
                            'val': b.get(qn('w:val')),
                            'sz': b.get(qn('w:sz')),
                            'color': b.get(qn('w:color'))
                        }
            
            # Background color
            shd = pPr.find(qn('w:shd'))
            if shd is not None:
                formatting['background'] = shd.get(qn('w:fill'))
    except Exception as e:
        logger.debug(f"Error extracting paragraph formatting: {e}")
    
    return formatting

def apply_paragraph_formatting(p_elem, formatting: Dict[str, Any]) -> None:
    """Apply paragraph-level formatting."""
    try:
        pPr = p_elem.find(qn('w:pPr'))
        if pPr is None:
            pPr = OxmlElement('w:pPr')
            p_elem.insert(0, pPr)
        
        # Alignment
        if 'alignment' in formatting:
            jc = pPr.find(qn('w:jc'))
            if jc is None:
                jc = OxmlElement('w:jc')
                pPr.append(jc)
            jc.set(qn('w:val'), formatting['alignment'])
        
        # Indentation
        if any(k in formatting for k in ['indent_left', 'indent_right', 'indent_first', 'hanging']):
            ind = pPr.find(qn('w:ind'))
            if ind is None:
                ind = OxmlElement('w:ind')
                pPr.append(ind)
            for attr, xml_attr in [
                ('indent_left', 'left'),
                ('indent_right', 'right'),
                ('indent_first', 'firstLine'),
                ('hanging', 'hanging')
            ]:
                if attr in formatting:
                    ind.set(qn(f'w:{xml_attr}'), formatting[attr])
        
        # Spacing
        if any(k in formatting for k in ['space_before', 'space_after', 'line_spacing', 'line_rule']):
            spacing = pPr.find(qn('w:spacing'))
            if spacing is None:
                spacing = OxmlElement('w:spacing')
                pPr.append(spacing)
            for attr, xml_attr in [
                ('space_before', 'before'),
                ('space_after', 'after'),
                ('line_spacing', 'line'),
                ('line_rule', 'lineRule')
            ]:
                if attr in formatting:
                    spacing.set(qn(f'w:{xml_attr}'), formatting[attr])
        
        # Borders
        if any(k.startswith('border_') for k in formatting):
            pBdr = pPr.find(qn('w:pBdr'))
            if pBdr is None:
                pBdr = OxmlElement('w:pBdr')
                pPr.append(pBdr)
            for border in ['top', 'left', 'bottom', 'right']:
                key = f'border_{border}'
                if key in formatting:
                    b = pBdr.find(qn(f'w:{border}'))
                    if b is None:
                        b = OxmlElement(f'w:{border}')
                        pBdr.append(b)
                    for attr in ['val', 'sz', 'color']:
                        if attr in formatting[key]:
                            b.set(qn(f'w:{attr}'), formatting[key][attr])
        
        # Background color
        if 'background' in formatting:
            shd = pPr.find(qn('w:shd'))
            if shd is None:
                shd = OxmlElement('w:shd')
                pPr.append(shd)
            shd.set(qn('w:fill'), formatting['background'])
            
    except Exception as e:
        logger.debug(f"Error applying paragraph formatting: {e}")

# ------------------------------
# Devanagari/ Hindi helpers
# ------------------------------
def apply_devanagari_to_first_run(r_elem, font_name='Noto Sans Devanagari', min_pt=11) -> None:
    try:
        rPr = r_elem.find(qn('w:rPr'))
        if rPr is None:
            rPr = OxmlElement('w:rPr')
            r_elem.insert(0, rPr)

        rFonts = rPr.find(qn('w:rFonts'))
        if rFonts is None:
            rFonts = OxmlElement('w:rFonts'); rPr.append(rFonts)
        rFonts.set(qn('w:ascii'), font_name)
        rFonts.set(qn('w:hAnsi'), font_name)
        rFonts.set(qn('w:cs'), font_name)
        rFonts.set(qn('w:eastAsia'), font_name)

        lang = rPr.find(qn('w:lang'))
        if lang is None:
            lang = OxmlElement('w:lang'); rPr.append(lang)
        try:
            lang.set(qn('w:val'), 'hi-IN')
            lang.set(qn('w:bidi'), 'hi-IN')
        except Exception:
            pass

        sz = rPr.find(qn('w:sz'))
        original_size_hpt = None
        if sz is not None and sz.get(qn('w:val')):
            try:
                original_size_hpt = int(sz.get(qn('w:val')))
            except Exception:
                original_size_hpt = None
        if original_size_hpt is None:
            target_pt = min_pt
        else:
            target_pt = max(min_pt, int(original_size_hpt / 2))

        target_hpt = str(int(target_pt * 2))
        sz_elem = rPr.find(qn('w:sz'))
        if sz_elem is None:
            sz_elem = OxmlElement('w:sz'); rPr.append(sz_elem)
        sz_elem.set(qn('w:val'), target_hpt)

        szCs_elem = rPr.find(qn('w:szCs'))
        if szCs_elem is None:
            szCs_elem = OxmlElement('w:szCs'); rPr.append(szCs_elem)
        szCs_elem.set(qn('w:val'), target_hpt)
    except Exception as e:
        logger.debug(f"apply_devanagari_to_first_run error: {e}")

def is_devanagari_text(text: str, fraction_threshold: float = 0.12) -> bool:
    if not text:
        return False
    dev_count = sum(1 for c in text if '\u0900' <= c <= '\u097F')
    alpha_count = sum(1 for c in text if c.isalpha())
    if alpha_count == 0:
        return False
    return (dev_count / alpha_count) >= fraction_threshold

def set_run_font_to_devanagari(r_elem, font_name: str = 'Noto Sans Devanagari', min_pt: Optional[int] = None) -> None:
    try:
        rPr = r_elem.find(qn('w:rPr'))
        if rPr is None:
            rPr = OxmlElement('w:rPr')
            r_elem.insert(0, rPr)

        rFonts = rPr.find(qn('w:rFonts'))
        if rFonts is None:
            rFonts = OxmlElement('w:rFonts'); rPr.append(rFonts)
        rFonts.set(qn('w:ascii'), font_name)
        rFonts.set(qn('w:hAnsi'), font_name)
        rFonts.set(qn('w:cs'), font_name)
        rFonts.set(qn('w:eastAsia'), font_name)

        lang = rPr.find(qn('w:lang'))
        if lang is None:
            lang = OxmlElement('w:lang'); rPr.append(lang)
        try:
            lang.set(qn('w:val'), 'hi-IN')
            lang.set(qn('w:bidi'), 'hi-IN')
        except Exception:
            pass

        if min_pt is not None:
            target_hpt = str(int(min_pt * 2))
            sz = rPr.find(qn('w:sz'))
            if sz is None:
                sz = OxmlElement('w:sz'); rPr.append(sz)
            sz.set(qn('w:val'), target_hpt)
            szCs = rPr.find(qn('w:szCs'))
            if szCs is None:
                szCs = OxmlElement('w:szCs'); rPr.append(szCs)
            szCs.set(qn('w:val'), target_hpt)
    except Exception as e:
        logger.debug(f"set_run_font_to_devanagari error: {e}")

def apply_noto_sans_devanagari(doc: Document, force: bool = False, min_pt: Optional[int] = 11) -> None:
    try:
        items = collect_text_items_all_parts(doc)
        seen_runs = set()
        heading_style_names = detect_heading_styles(doc)
        
        for it in items:
            text = it.get('text') or ''
            r_nodes = it.get('r_nodes') or []
            is_heading = is_heading_item(it, heading_style_names)
            
            # Skip empty text or non-Devanagari text (unless forced)
            if not force and not is_devanagari_text(text):
                continue
                
            for r in r_nodes:
                if id(r) in seen_runs:
                    continue
                    
                # For headings, preserve original font and just set language
                if is_heading:
                    try:
                        rPr = r.find(qn('w:rPr'))
                        if rPr is None:
                            rPr = OxmlElement('w:rPr')
                            r.insert(0, rPr)
                        
                        # Set language but preserve font
                        lang = rPr.find(qn('w:lang'))
                        if lang is None:
                            lang = OxmlElement('w:lang')
                            rPr.append(lang)
                        lang.set(qn('w:val'), 'hi-IN')
                        lang.set(qn('w:bidi'), 'hi-IN')
                    except Exception as e:
                        logger.debug(f"Error setting heading language: {e}")
                else:
                    # For regular text, apply Noto Sans Devanagari
                    set_run_font_to_devanagari(r, font_name='Noto Sans Devanagari', min_pt=min_pt)
                seen_runs.add(id(r))

        # Removed redundant python-docx traversal when force=True
                    
    except Exception as e:
        logger.debug(f"apply_noto_sans_devanagari overall failure: {e}")

# ------------------------------
# Table and Textbox Handling
# ------------------------------

def get_table_cell_formatting(tc_elem) -> Dict[str, Any]:
    """Extract table cell formatting."""
    formatting = {}
    try:
        tcPr = tc_elem.find(qn('w:tcPr'))
        if tcPr is not None:
            # Vertical alignment
            vAlign = tcPr.find(qn('w:vAlign'))
            if vAlign is not None:
                formatting['vertical_align'] = vAlign.get(qn('w:val'))
            
            # Width
            tcW = tcPr.find(qn('w:tcW'))
            if tcW is not None:
                formatting['width'] = tcW.get(qn('w:w'))
                formatting['width_type'] = tcW.get(qn('w:type'))
            
            # Borders
            tcBorders = tcPr.find(qn('w:tcBorders'))
            if tcBorders is not None:
                for border in ['top', 'left', 'bottom', 'right']:
                    b = tcBorders.find(qn(f'w:{border}'))
                    if b is not None:
                        formatting[f'border_{border}'] = {
                            'val': b.get(qn('w:val')),
                            'sz': b.get(qn('w:sz')),
                            'color': b.get(qn('w:color'))
                        }
            
            # Background color
            shd = tcPr.find(qn('w:shd'))
            if shd is not None:
                formatting['background'] = shd.get(qn('w:fill'))
            
            # Margins
            margins = tcPr.find(qn('w:tcMar'))
            if margins is not None:
                for side in ['top', 'left', 'bottom', 'right']:
                    m = margins.find(qn(f'w:{side}'))
                    if m is not None:
                        formatting[f'margin_{side}'] = {
                            'w': m.get(qn('w:w')),
                            'type': m.get(qn('w:type'))
                        }
    except Exception as e:
        logger.debug(f"Error extracting table cell formatting: {e}")
    
    return formatting

def apply_table_cell_formatting(tc_elem, formatting: Dict[str, Any]) -> None:
    """Apply table cell formatting."""
    try:
        tcPr = tc_elem.find(qn('w:tcPr'))
        if tcPr is None:
            tcPr = OxmlElement('w:tcPr')
            tc_elem.insert(0, tcPr)
        
        # Vertical alignment
        if 'vertical_align' in formatting:
            vAlign = tcPr.find(qn('w:vAlign'))
            if vAlign is None:
                vAlign = OxmlElement('w:vAlign')
                tcPr.append(vAlign)
            vAlign.set(qn('w:val'), formatting['vertical_align'])
        
        # Width
        if 'width' in formatting:
            tcW = tcPr.find(qn('w:tcW'))
            if tcW is None:
                tcW = OxmlElement('w:tcW')
                tcPr.append(tcW)
            tcW.set(qn('w:w'), formatting['width'])
            if 'width_type' in formatting:
                tcW.set(qn('w:type'), formatting['width_type'])
        
        # Borders
        if any(k.startswith('border_') for k in formatting):
            tcBorders = tcPr.find(qn('w:tcBorders'))
            if tcBorders is None:
                tcBorders = OxmlElement('w:tcBorders')
                tcPr.append(tcBorders)
            for border in ['top', 'left', 'bottom', 'right']:
                key = f'border_{border}'
                if key in formatting:
                    b = tcBorders.find(qn(f'w:{border}'))
                    if b is None:
                        b = OxmlElement(f'w:{border}')
                        tcBorders.append(b)
                    for attr in ['val', 'sz', 'color']:
                        if attr in formatting[key]:
                            b.set(qn(f'w:{attr}'), formatting[key][attr])
        
        # Background color
        if 'background' in formatting:
            shd = tcPr.find(qn('w:shd'))
            if shd is None:
                shd = OxmlElement('w:shd')
                tcPr.append(shd)
            shd.set(qn('w:fill'), formatting['background'])
        
        # Margins
        if any(k.startswith('margin_') for k in formatting):
            margins = tcPr.find(qn('w:tcMar'))
            if margins is None:
                margins = OxmlElement('w:tcMar')
                tcPr.append(margins)
            for side in ['top', 'left', 'bottom', 'right']:
                key = f'margin_{side}'
                if key in formatting:
                    m = margins.find(qn(f'w:{side}'))
                    if m is None:
                        m = OxmlElement(f'w:{side}')
                        margins.append(m)
                    m.set(qn('w:w'), formatting[key]['w'])
                    if 'type' in formatting[key]:
                        m.set(qn('w:type'), formatting[key]['type'])
    except Exception as e:
        logger.debug(f"Error applying table cell formatting: {e}")

def process_table_cell(cell_elem) -> List[Dict[str, Any]]:
    """Process a table cell and extract its content with formatting."""
    items = []
    try:
        # Get cell formatting
        cell_format = get_table_cell_formatting(cell_elem)
        
        # Process paragraphs in cell
        for p in cell_elem.xpath('.//w:p', namespaces=_NS):
            # Get paragraph formatting
            para_format = get_paragraph_formatting(p)
            
            # Process runs in paragraph
            segs = _segment_paragraph_items(p, use_local_names=False)
            for seg in segs:
                item = {
                    'text': seg['text'],
                    't_nodes': seg['t_nodes'],
                    'r_nodes': seg['r_nodes'],
                    'para': p,
                    'cell': cell_elem,
                    'cell_format': cell_format,
                    'para_format': para_format
                }
                
                # Get run formatting for each run
                run_formats = []
                for r in seg['r_nodes']:
                    run_formats.append(get_run_formatting(r))
                item['run_formats'] = run_formats
                
                items.append(item)
    except Exception as e:
        logger.debug(f"Error processing table cell: {e}")
    
    return items

def process_textbox(txbx_elem) -> List[Dict[str, Any]]:
    """Process a textbox and extract its content with formatting."""
    items = []
    try:
        # Find the actual content container
        content = txbx_elem.find('.//w:txbxContent', namespaces=_NS)
        if content is None:
            return items
        
        # Process paragraphs in textbox
        for p in content.xpath('.//w:p', namespaces=_NS):
            # Get paragraph formatting
            para_format = get_paragraph_formatting(p)
            
            # Process runs in paragraph
            segs = _segment_paragraph_items(p, use_local_names=False)
            for seg in segs:
                item = {
                    'text': seg['text'],
                    't_nodes': seg['t_nodes'],
                    'r_nodes': seg['r_nodes'],
                    'para': p,
                    'textbox': txbx_elem,
                    'para_format': para_format
                }
                
                # Get run formatting for each run
                run_formats = []
                for r in seg['r_nodes']:
                    run_formats.append(get_run_formatting(r))
                item['run_formats'] = run_formats
                
                items.append(item)
    except Exception as e:
        logger.debug(f"Error processing textbox: {e}")
    
    return items

# ------------------------------
# Parts iterator, collector, chunking, translation wrapper, etc.
# ------------------------------

def _all_parts(doc: Document):
    yield doc.part
    for s in doc.sections:
        try:
            if s.header is not None and getattr(s.header, "part", None) is not None:
                yield s.header.part
        except Exception:
            pass
        try:
            if s.footer is not None and getattr(s.footer, "part", None) is not None:
                yield s.footer.part
        except Exception:
            pass

def _segment_paragraph_items(p_elem, use_local_names: bool = False) -> List[Dict[str, Any]]:
    try:
        if use_local_names:
            r_nodes_all = p_elem.xpath('.//*[local-name()="r"]')
            t_in_run_xpath = './/*[local-name()="t"]'
            br_in_run_xpath = './/*[local-name()="br"]|.//*[local-name()="tab"]'
            hyperlink_anc_xpath = 'ancestor::*[local-name()="hyperlink"][1]'
        else:
            r_nodes_all = p_elem.xpath('.//w:r', namespaces=_NS)
            t_in_run_xpath = './/w:t'
            br_in_run_xpath = './/w:br|.//w:tab'
            hyperlink_anc_xpath = 'ancestor::w:hyperlink[1]'

        if not r_nodes_all:
            if use_local_names:
                t_nodes = p_elem.xpath('.//*[local-name()="t"]')
                r_nodes = p_elem.xpath('.//*[local-name()="r"]')
            else:
                t_nodes = p_elem.xpath('.//w:t', namespaces=_NS)
                r_nodes = p_elem.xpath('.//w:r', namespaces=_NS)
            joined = ''.join([t.text or '' for t in t_nodes]) if t_nodes else ''
            t_space_flags = []
            for t in t_nodes:
                tx = t.text or ''
                t_space_flags.append({'leading': bool(tx[:1].isspace()), 'trailing': bool(tx[-1:].isspace())})
            return [{'text': joined, 't_nodes': t_nodes, 'r_nodes': r_nodes, 't_space_flags': t_space_flags}]

        segments: List[Dict[str, Any]] = []
        current_key_id = None
        current_t_nodes: List[Any] = []
        current_r_nodes: List[Any] = []

        def _finalize_current():
            nonlocal current_t_nodes, current_r_nodes, current_key_id
            if not current_t_nodes and not current_r_nodes:
                current_key_id = None
                return
            joined = ''.join((t.text or '') for t in current_t_nodes)
            seen = set()
            r_nodes: List[Any] = []
            for r in current_r_nodes:
                if id(r) not in seen:
                    r_nodes.append(r); seen.add(id(r))
            t_space_flags = []
            for t in current_t_nodes:
                tx = t.text or ''
                t_space_flags.append({'leading': bool(tx[:1].isspace()), 'trailing': bool(tx[-1:].isspace())})
            segments.append({'text': joined, 't_nodes': list(current_t_nodes), 'r_nodes': r_nodes, 't_space_flags': t_space_flags})
            current_t_nodes = []; current_r_nodes = []
            current_key_id = None

        def _localname(tag: str) -> str:
            return tag.split('}', 1)[-1] if '}' in tag else tag

        for r in r_nodes_all:
            anc = r.xpath(hyperlink_anc_xpath, namespaces=_NS) if not use_local_names else r.xpath(hyperlink_anc_xpath)
            key_elem = anc[0] if anc else None
            key_id = id(key_elem) if key_elem is not None else None

            # Keep hyperlink spans atomic: finalize when hyperlink ancestor changes
            if current_t_nodes and (current_key_id is not None and key_id is not None and key_id != current_key_id):
                _finalize_current()

            # Iterate run children to split exactly at tabs/breaks
            for child in list(r):
                lname = _localname(child.tag)
                if lname == 't':
                    current_t_nodes.append(child)
                    current_r_nodes.append(r)
                    current_key_id = key_id
                elif lname in ('br', 'tab'):
                    if current_t_nodes:
                        _finalize_current()
                    # After a break, continue collecting further text in the same run as a new segment
                    current_key_id = key_id
                else:
                    # ignore other child types
                    continue

        _finalize_current()
        return segments
    except Exception:
        try:
            if use_local_names:
                t_nodes = p_elem.xpath('.//*[local-name()="t"]')
                r_nodes = p_elem.xpath('.//*[local-name()="r"]')
            else:
                t_nodes = p_elem.xpath('.//w:t', namespaces=_NS)
                r_nodes = p_elem.xpath('.//w:r', namespaces=_NS)
            joined = ''.join([t.text or '' for t in t_nodes]) if t_nodes else ''
            t_space_flags = []
            for t in t_nodes:
                tx = t.text or ''
                t_space_flags.append({'leading': bool(tx[:1].isspace()), 'trailing': bool(tx[-1:].isspace())})
            return [{'text': joined, 't_nodes': t_nodes, 'r_nodes': r_nodes, 't_space_flags': t_space_flags}]
        except Exception:
            return []

def collect_text_items_all_parts(doc: Document) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    idx = 0
    para_paths_ns = [
        './/w:p',
        './/wps:txbx/w:txbxContent//w:p',
        './/w:pict//v:shape//v:textbox//w:txbxContent//w:p',
        './/w:sdt//w:sdtContent//w:p'
    ]
    para_paths_local = [
        './/*[local-name()="p"]',
        './/*[local-name()="txbxContent"]//*[local-name()="p"]',
        './/*[local-name()="pict"]//*[local-name()="shape"]//*[local-name()="textbox"]//*[local-name()="txbxContent"]//*[local-name()="p"]',
        './/*[local-name()="sdt"]//*[local-name()="sdtContent"]//*[local-name()="p"]'
    ]

    for part in _all_parts(doc):
        root = part.element
        found_in_part = 0

        def _extract_style_value(p_elem) -> Optional[str]:
            try:
                vals = p_elem.xpath('.//w:pPr/w:pStyle/@w:val', namespaces=_NS)
                if vals:
                    return str(vals[0])
            except Exception:
                pass
            try:
                vals_local = p_elem.xpath('.//*[local-name()="pPr"]/*[local-name()="pStyle"]/@*')
                for attr in vals_local:
                    try:
                        return str(attr)
                    except Exception:
                        continue
            except Exception:
                pass
            return None

        for path in para_paths_ns:
            try:
                matches = root.xpath(path, namespaces=_NS)
                for p in matches:
                    style_val = _extract_style_value(p)
                    segs = _segment_paragraph_items(p, use_local_names=False)
                    for seg in segs:
                        items.append({'id': f'P{idx}', 'text': seg['text'], 'style': style_val or '', 'part': part, 'para': p, 't_nodes': seg['t_nodes'], 'r_nodes': seg['r_nodes'], 't_space_flags': seg.get('t_space_flags', [])})
                        idx += 1
                if matches:
                    found_in_part += len(matches)
            except Exception as e:
                logger.debug(f"Namespace-aware xpath error on part {getattr(part, 'partname', getattr(part, 'content_type', 'unknown'))}: {e}")

        if found_in_part == 0:
            for path in para_paths_local:
                try:
                    matches = root.xpath(path)
                    for p in matches:
                        style_val = _extract_style_value(p)
                        segs = _segment_paragraph_items(p, use_local_names=True)
                        for seg in segs:
                            items.append({'id': f'P{idx}', 'text': seg['text'], 'style': style_val or '', 'part': part, 'para': p, 't_nodes': seg['t_nodes'], 'r_nodes': seg['r_nodes'], 't_space_flags': seg.get('t_space_flags', [])})
                            idx += 1
                    if matches:
                        found_in_part += len(matches)
                except Exception as e:
                    logger.debug(f"Local-name xpath error on part {getattr(part, 'partname', getattr(part, 'content_type', 'unknown'))}: {e}")

        logger.info(f"collect_text_items_all_parts: part {getattr(part, 'partname', getattr(part, 'content_type', 'unknown'))} -> paragraphs found: {found_in_part}")

    if not items:
        logger.info("No items found via XPath. Falling back to python-docx traversal (body, headers, footers, tables).")

        def _collect_from_container(container):
            nonlocal idx
            for para in getattr(container, 'paragraphs', []):
                try:
                    p_elem = para._p
                except Exception:
                    continue
                style_val = None
                try:
                    if getattr(para, 'style', None) is not None and getattr(para.style, 'name', None) is not None:
                        style_val = str(para.style.name)
                except Exception:
                    style_val = None
                segs = _segment_paragraph_items(p_elem, use_local_names=False)
                for seg in segs:
                    items.append({'id': f'P{idx}', 'text': seg['text'], 'style': style_val or '', 'part': getattr(container, 'part', None), 'para': p_elem, 't_nodes': seg['t_nodes'], 'r_nodes': seg['r_nodes'], 't_space_flags': seg.get('t_space_flags', [])})
                    idx += 1
            for table in getattr(container, 'tables', []):
                for row in table.rows:
                    for cell in row.cells:
                        _collect_from_container(cell)

        _collect_from_container(doc)
        for section in doc.sections:
            try:
                if section.header:
                    _collect_from_container(section.header)
                if section.footer:
                    _collect_from_container(section.footer)
            except Exception:
                pass

        logger.info(f"Fallback traversal collected {len(items)} items via python-docx API.")

    return items

def merge_segments_by_para(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not items:
        return []
    merged: List[Dict[str, Any]] = []
    prev_para_id = None
    current = None
    for it in items:
        para = it.get('para')
        key = id(para) if para is not None else None
        if key == prev_para_id and current is not None:
            # append text and extend t_nodes/r_nodes
            current['text'] = (current.get('text') or '') + (it.get('text') or '')
            current['t_nodes'].extend(it.get('t_nodes') or [])
            current['r_nodes'].extend(it.get('r_nodes') or [])
        else:
            current = {k: (v.copy() if isinstance(v, list) else v) for k, v in it.items()}
            # ensure lists exist
            current.setdefault('t_nodes', current.get('t_nodes') or [])
            current.setdefault('r_nodes', current.get('r_nodes') or [])
            merged.append(current)
            prev_para_id = key
    return merged

def _para_has_page_break(para) -> bool:
    try:
        brs = para.xpath('.//w:br[@w:type="page"]', namespaces=_NS)
        if brs:
            return True
        sect = para.xpath('./w:pPr/w:sectPr', namespaces=_NS)
        if sect:
            return True
    except Exception:
        pass
    return False

def detect_heading_styles(doc: Document) -> List[str]:
    heading_styles = []
    try:
        # First, get built-in heading styles
        for i in range(1, 6):  # Heading levels 1-5
            heading_styles.append(f'Heading {i}')
            heading_styles.append(f'heading {i}')
            heading_styles.append(f'HEADING {i}')
        
        # Then add custom heading styles from the document
        for style in doc.styles:
            try:
                style_name = str(style.name or '').lower()
                if getattr(style, 'type', None) == 1 and style_name:
                    # Check for heading in name or based on style type
                    if ('heading' in style_name or 
                        'title' in style_name or 
                        'header' in style_name or
                        getattr(style.base_style, 'name', '').lower().startswith('heading')):
                        heading_styles.append(style.name)
            except Exception:
                continue
    except Exception as e:
        logger.debug(f"Error detecting heading styles: {e}")
    
    return list(set(heading_styles))  # Remove duplicates

def get_heading_level(style_name: str) -> Optional[int]:
    """Get the heading level from a style name, returns None if not a heading."""
    if not style_name:
        return None
    
    style_lower = style_name.lower()
    
    # Check for standard heading styles
    for i in range(1, 10):
        if style_lower in [f'heading {i}', f'heading{i}', f'h{i}']:
            return i
    
    # Check for custom heading styles with numbers
    if 'heading' in style_lower:
        for i in range(1, 10):
            if str(i) in style_lower:
                return i
    
    # Title is typically equivalent to Heading 1
    if 'title' in style_lower:
        return 1
    
    return None

def is_heading_item(item: Dict[str, Any], heading_style_names: List[str]) -> bool:
    """Check if an item is a heading and get its level."""
    style_val = item.get('style', '')
    
    # First check if it's a known heading style
    if style_val:
        # Direct style name match
        if style_val in heading_style_names:
            return True
        # Get heading level
        if get_heading_level(style_val) is not None:
            return True
    
    # Check for additional heading indicators
    text_val = (item.get('text') or '').strip()
    if text_val:
        # Check for all-caps short titles (potential unlabeled headings)
        if 0 < len(text_val) <= 120 and text_val.replace(' ', '').isupper() and len(text_val) > 2:
            return True
        
        # Check for numbered headings without proper style
        if re.match(r'^\d+(\.\d+)*\s+[A-Z]', text_val):
            return True
    
    return False

def create_smart_chunks(items: List[Dict[str, Any]], doc: Document, max_chars: int = 7000) -> List[List[Dict[str, Any]]]:
    """Create chunks based on document structure and content, not page breaks."""
    if not items:
        return []
    
    # Merge segments and get heading styles
    items = merge_segments_by_para(items)
    heading_style_names = detect_heading_styles(doc)
    
    chunks: List[List[Dict[str, Any]]] = []
    current_chunk: List[Dict[str, Any]] = []
    current_len = 0
    
    def finalize_chunk():
        nonlocal current_chunk, current_len
        if current_chunk:
            chunks.append(current_chunk)
            current_chunk = []
            current_len = 0
    
    for i, item in enumerate(items):
        text = item.get('text') or ''
        text_len = len(text)
        is_heading = is_heading_item(item, heading_style_names)
        
        # Start new chunk on heading and isolate the heading as its own chunk
        if is_heading:
            if current_chunk:
                finalize_chunk()
            # push heading as its own chunk to ensure clean translation
            chunks.append([item])
            continue
        
        # Check if adding this item would exceed max_chars
        if current_len + text_len > max_chars and current_chunk:
            # Try to find a good break point in current_chunk
            break_idx = len(current_chunk) - 1
            while break_idx > 0:
                if is_heading_item(current_chunk[break_idx], heading_style_names):
                    # Found a heading to break at
                    new_chunk = current_chunk[:break_idx]
                    leftover = current_chunk[break_idx:]
                    if new_chunk:
                        chunks.append(new_chunk)
                    current_chunk = leftover
                    current_len = sum(len(it.get('text') or '') for it in leftover)
                    break
                break_idx -= 1
            else:
                # No good break point found, just split here
                finalize_chunk()
        
        # Add item to current chunk
        current_chunk.append(item)
        current_len += text_len
        
        # Special handling for last item
        if i == len(items) - 1:
            finalize_chunk()
    
    # Post-process chunks to ensure they're not too small
    final_chunks: List[List[Dict[str, Any]]] = []
    min_chunk_size = max_chars // 2  # Minimum chunk size is 1/3 of max
    
    current: List[Dict[str, Any]] = []
    current_size = 0
    
    for chunk in chunks:
        chunk_size = sum(len(it.get('text') or '') for it in chunk)
        
        if current and current_size + chunk_size <= max_chars:
            # Can merge with previous chunk
            current.extend(chunk)
            current_size += chunk_size
        else:
            # Start new chunk
            if current:
                if current_size < min_chunk_size and len(final_chunks) > 0:
                    # Merge small chunk with previous only if within max_chars
                    last_chunk_size = sum(len(it.get('text') or '') for it in final_chunks[-1])
                    if last_chunk_size + current_size <= max_chars:
                        final_chunks[-1].extend(current)
                    else:
                        final_chunks.append(current)
                else:
                    final_chunks.append(current)
            current = chunk
            current_size = chunk_size
    
    # Handle last chunk with max size check to avoid exceeding max_chars
    if current:
        if current_size < min_chunk_size and len(final_chunks) > 0:
            last_chunk_size = sum(len(it.get('text') or '') for it in final_chunks[-1])
            if last_chunk_size + current_size <= max_chars:
                final_chunks[-1].extend(current)
            else:
                final_chunks.append(current)
        else:
            final_chunks.append(current)
    
    logger.info(f"create_smart_chunks => chunks created: {len(final_chunks)}")
    return final_chunks

def check_api_keys() -> Dict[str, bool]:
    groq_key = os.environ.get("GROQ_API_KEY") or os.environ.get("GROQ_KEY")
    gemini_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if not groq_key and not gemini_key:
        raise RuntimeError("No API keys found. Set GROQ_API_KEY or GEMINI_API_KEY environment variables.")
    if groq_key:
        logger.info("Groq API key found")
    if gemini_key:
        logger.info("Gemini API key found")
    return {"groq": bool(groq_key), "gemini": bool(gemini_key)}

def _make_cache_key(engine: str, model_name: str, temperature: float, target_language: str, items: List[Dict[str, Any]]) -> str:
    try:
        serializable = [
            {
                "id": it.get("id"),
                "text": it.get("text", ""),
                "style": it.get("style", "")
            }
            for it in items
        ]
        packed = json.dumps(serializable, ensure_ascii=False, separators=(',', ':'), sort_keys=True)
        h = hashlib.sha256(packed.encode("utf-8")).hexdigest()
        return f"{engine}|{model_name}|{temperature}|{target_language}|{h}"
    except Exception:
        try:
            ids_concat = "|".join([str(it.get("id")) for it in items])
            h = hashlib.sha256(ids_concat.encode("utf-8")).hexdigest()
            return f"{engine}|{model_name}|{temperature}|{target_language}|ids-{h}"
        except Exception:
            return f"{engine}|{model_name}|{temperature}|{target_language}|nocache"

def _extract_json_from_text(output_text: str) -> Optional[Any]:
    out = output_text.strip()
    try:
        return json.loads(out)
    except Exception:
        pass
    first = None
    for i, ch in enumerate(out):
        if ch in ('[', '{'):
            first = i
            break
    if first is None:
        return None
    for j in range(len(out) - 1, first - 1, -1):
        candidate = out[first:j + 1]
        try:
            return json.loads(candidate)
        except Exception:
            continue
    return None

def _ensure_client(engine: str):
    global groq_client, gemini_client
    if engine.lower() == 'groq':
        if groq_client is None:
            if GroqClient is None:
                raise RuntimeError("GroqClient not available.")
            groq_client = GroqClient()
            try:
                groq_client.set_model("llama-3.3-70b-versatile")
            except Exception:
                pass
        return groq_client, getattr(groq_client, 'model_name', 'groq-default')
    elif engine.lower() == 'gemini':
        if gemini_client is None:
            if GeminiClient is None:
                raise RuntimeError("GeminiClient not available.")
            gemini_client = GeminiClient()
            try:
                gemini_client.set_model("gemini-2.5-flash-lite")
            except Exception:
                pass
        return gemini_client, getattr(gemini_client, 'model_name', 'gemini-default')
    else:
        raise ValueError(f"Unsupported engine: {engine}.")

def _invoke_model(client, payload: str, system_prompt: str, temperature: float) -> str:
    tried = []
    candidates = [
        ("process_batch", ([payload],)),  # process_batch already handles temperature internally
        ("process_sequential", ([payload], None, temperature)),  # fallback with explicit temperature
        ("generate", (payload,)),
        ("run", (payload,)),
        ("chat", (payload,)),
        ("send", (payload,)),
    ]
    for method_name, params in candidates:
        try:
            fn = getattr(client, method_name, None)
            if not fn:
                tried.append(method_name + ":missing")
                continue
            res = fn(*params)
            if isinstance(res, list):
                if not res:
                    return ""
                out = res[0]
            elif isinstance(res, dict):
                out = res.get('output') or res.get('text') or next(iter(res.values()), '')
            else:
                out = res
            out_s = str(out)
            return out_s
        except TypeError:
            tried.append(method_name + ":typeerror")
            continue
        except Exception as e:
            tried.append(f"{method_name}:err:{e}")
            continue
    try:
        res = client(payload)
        return str(res)
    except Exception:
        logger.debug("Model invocation tried methods: %s", tried)
        raise RuntimeError(f"Failed to invoke model client. Tried: {tried}")

def translate_text_json(items: List[Dict[str, str]], engine: str = 'groq', target_language: str = 'hi', temperature: float = 0.7, max_chars_per_chunk: int = 8000, doc: Optional[Document] = None) -> Dict[str, str]:
    if not isinstance(items, list) or not items:
        return {}

    client, model_name = _ensure_client(engine)

    protected_items: List[Dict[str, str]] = []
    placeholders_map: Dict[str, Dict[str, str]] = {}
    # Pre-compute heading styles once
    doc_for_headings = doc
    heading_style_names = detect_heading_styles(doc_for_headings) if doc_for_headings else []

    for it in items:
        txt = (it.get('text', '') or '')
        if not txt.strip():
            protected_items.append({'id': it['id'], 'text': ''})
            placeholders_map[it['id']] = {}
            continue
        # Do not protect acronyms/abbreviations; only optionally skip numbers-in-units in headings
        skip_tags = {'NUMUNIT'} if is_heading_item(it, heading_style_names) else set()
        p, ph = protect_tokens(txt, skip_tags=skip_tags)
        new_it = {'id': it['id'], 'text': p}
        if 'style' in it:
            new_it['style'] = it['style']
        if 'para' in it:
            new_it['para'] = it['para']
        protected_items.append(new_it)
        placeholders_map[it['id']] = ph

    chunk_source = protected_items

    # Always use smart chunking
    chunks = create_smart_chunks(chunk_source, doc, max_chars=max_chars_per_chunk)

    logger.info(f"translate_text_json => estimated chunks (API calls): {len(chunks)}")

    result_map: Dict[str, str] = {}

    def _call_and_validate(batch: List[Dict[str, Any]]) -> Dict[str, str]:
        minimal_batch = [{"id": it.get("id"), "text": it.get("text", "")} for it in batch]
        cache_key = _make_cache_key(engine, model_name, temperature, target_language, minimal_batch)

        cached = get_cached_translation(cache_key)
        if cached:
            logger.debug("Cache hit for batch")
            return cached

        in_ids_ordered = [it['id'] for it in batch]
        id_list_str = json.dumps(in_ids_ordered, ensure_ascii=False)
        expected_count = len(in_ids_ordered)

        system_prompt = (
            f"""YOU ARE A PRECISE, FAITHFUL TRANSLATOR. RETURN ONLY valid JSON (no code fences) as an array of exactly {expected_count} objects.
            Each object MUST have keys 'id' and 'text' only. The 'id' MUST be one of: {id_list_str}. Do NOT add, remove, or reorder ids.
            Translate each 'text' into {target_language} preserving meaning.
            SPACING:
            - Do not add or remove necessary spaces; keep single spaces where present.
            - Do not insert spaces inside words or before punctuation.
            - The numbers should also be translated into {target_language} numbering format. 
            - Keep placeholders exactly as-is (e.g., <URL_0>) without extra spaces around them.
            HINDI:
            - Do not insert spaces between Devanagari letters; natural punctuation spacing only.
            ACRONYMS/ABBREVIATIONS:
            - Translate acronyms and abbreviations; preserve original casing.
            - Do not add periods to acronyms unless present in the source.
    
            - Do NOT summarize or rewrite; translate fully. Preserve punctuation, capitalization (ALL CAPS headings), bullets, numbering, symbols, and layout cues.
            FOLLOW THESE INSTRUCTIONS CAREFULLY AND STRICTLY.
            """
        )
        input_json = json.dumps(minimal_batch, ensure_ascii=False)
        payload = system_prompt + "\n\nINPUT_JSON:\n" + input_json

        attempts = 3
        for attempt in range(attempts):
            try:
                rate_limit_wait()
                raw_out = _invoke_model(client, payload, system_prompt, temperature=temperature)
                if not raw_out:
                    raise RuntimeError("Empty response from model")
                raw_out = unescape(str(raw_out)).strip()
                parsed = _extract_json_from_text(raw_out)
                if parsed is None:
                    raise ValueError("Failed to extract JSON from model output.")
                parsed_list = parsed if isinstance(parsed, list) else [parsed]
                in_ids_set = set(in_ids_ordered)
                out_map_raw: Dict[str, str] = {}
                for o in parsed_list:
                    if not isinstance(o, dict) or 'id' not in o or 'text' not in o:
                        continue
                    oid = o['id']
                    if oid in in_ids_set and oid not in out_map_raw:
                        out_map_raw[oid] = o['text']
                missing_ids = [iid for iid in in_ids_ordered if iid not in out_map_raw]
                if missing_ids or (len(out_map_raw) != len(in_ids_set)):
                    logger.warning(f"ID reconciliation: filtered extras and filled {len(missing_ids)} missing ids.")
                if missing_ids:
                    src_map = {it['id']: it['text'] for it in batch}
                    for mid in missing_ids:
                        out_map_raw[mid] = src_map.get(mid, '')

                final_map: Dict[str, str] = {}
                for k in in_ids_ordered:
                    v = out_map_raw.get(k, next((it['text'] for it in batch if it['id'] == k), ''))
                    # Localize digits before restoring tokens to avoid touching placeholders
                    localized = localize_digits_outside_placeholders(v, target_language)
                    restored = restore_tokens(localized, placeholders_map.get(k, {}))
                    final_map[k] = restored
                set_cached_translation(cache_key, final_map)
                return final_map
            except Exception as e:
                logger.warning(f"Model attempt {attempt+1}/{attempts} failed for batch: {e}")
                time.sleep(1 + attempt * 2)
                if attempt == attempts - 1:
                    raise
        raise RuntimeError("Unreachable code in _call_and_validate")

    for chunk in chunks:
        try:
            translated_chunk_map = _call_and_validate(chunk)
            result_map.update(translated_chunk_map)
        except Exception as e:
            logger.warning(f"Chunk failed, attempting split retries: {e}")
            if len(chunk) <= 1:
                for it in chunk:
                    result_map[it['id']] = restore_tokens(it['text'], placeholders_map.get(it['id'], {}))
                continue
            mid = len(chunk) // 2
            halves = [chunk[:mid], chunk[mid:]]
            for half in halves:
                try:
                    translated_half = _call_and_validate(half)
                    result_map.update(translated_half)
                except Exception as e2:
                    logger.error(f"Failed sub-chunk: {e2}. Falling back to original text for these ids.")
                    for it in half:
                        result_map[it['id']] = restore_tokens(it['text'], placeholders_map.get(it['id'], {}))
                    continue

    return result_map

def distribute_text_across_t_nodes(translated_text: str, t_nodes: List[Any]) -> List[str]:
    if not t_nodes:
        return [translated_text]
    original_lens = []
    for t in t_nodes:
        try:
            original_lens.append(len(t.text or ''))
        except Exception:
            original_lens.append(0)
    total_orig = sum(original_lens)
    N = len(t_nodes)
    if total_orig <= 0:
        words = translated_text.split()
        if len(words) < N:
            parts = [''] * N
            parts[0] = translated_text
            return parts
        per = max(1, len(words) // N)
        parts = []
        i = 0
        for k in range(N):
            if k == N - 1:
                parts.append(' '.join(words[i:]))
            else:
                parts.append(' '.join(words[i:i+per]))
                i += per
        return parts

    # Token-based split: alternate between runs by tokens to avoid mid-word splits
    tokens = re.findall(r'\s+|\S+', translated_text)
    if tokens:
        # target token counts proportional to original lengths
        total_tokens = len(tokens)
        desired_tok = [max(1, int(round((l / total_orig) * total_tokens))) for l in original_lens]
        drift = total_tokens - sum(desired_tok)
        i_adj = 0
        while drift != 0 and N > 0:
            desired_tok[i_adj % N] += 1 if drift > 0 else -1
            drift = total_tokens - sum(desired_tok)
            i_adj += 1
        parts = []
        t_idx = 0
        for k in range(N):
            take = desired_tok[k]
            if k == N - 1:
                seg_toks = tokens[t_idx:]
            else:
                seg_toks = tokens[t_idx:t_idx + take]
            parts.append(''.join(seg_toks))
            t_idx += take
        while len(parts) < N:
            parts.append('')
        return parts

    parts = []
    desired = [max(1, int(round((l / total_orig) * len(translated_text)))) for l in original_lens]
    total_desired = sum(desired)
    diff = len(translated_text) - total_desired
    i = 0
    while diff != 0:
        desired[i % N] += 1 if diff > 0 else -1
        diff = len(translated_text) - sum(desired)
        i += 1
    idx = 0
    for L in desired:
        if idx + L >= len(translated_text):
            part = translated_text[idx:]
            parts.append(part)
            idx = len(translated_text)
            break
        end = idx + L
        # Prefer to break at whitespace; if not found, search a bit earlier then later
        if end < len(translated_text) and not translated_text[end].isspace():
            # look back up to 12 chars for a whitespace to avoid breaking words
            back_span_start = max(idx + 1, end - 12)
            back_space = -1
            for j in range(end, back_span_start - 1, -1):
                if translated_text[j - 1].isspace():
                    back_space = j
                    break
            if back_space != -1 and back_space > idx:
                end = back_space
            else:
                # look forward up to 12 chars for whitespace
                fwd_span_end = min(len(translated_text), end + 12)
                fwd_space = translated_text.find(' ', end, fwd_span_end)
                if fwd_space != -1:
                    end = fwd_space
        part = translated_text[idx:end]
        parts.append(part)
        idx = end
    while len(parts) < N:
        parts.append('')
    return parts

def apply_translation_to_item(item: Dict[str, Any], translated_text: str, target_language: str = 'hi', heading_style_names: Optional[List[str]] = None) -> None:
    """Apply translation while preserving all formatting."""
    try:
        t_nodes = item.get('t_nodes', []) or []
        r_nodes = item.get('r_nodes', []) or []
        if not t_nodes:
            return
            
        # Get all formatting information
        run_formats = item.get('run_formats', [])
        para_format = item.get('para_format', {})
        cell_format = item.get('cell_format', {})
        
        # Distribute translated text across nodes
        try:
            parts = distribute_text_across_t_nodes(translated_text, t_nodes)
            
            # Apply text and preserve formatting for each node
            for i, (t_node, ptext) in enumerate(zip(t_nodes, parts)):
                if ptext is None:
                    ptext = ''
                    
                # Set text content
                t_node.text = ptext
                ensure_textnode_preserve_space(t_node)
                
                # Apply run formatting if available. If segmenting split a run, fallback to first run's format.
                if i < len(r_nodes) and i < len(run_formats):
                    apply_run_formatting(r_nodes[i], run_formats[i])
                elif r_nodes and run_formats:
                    apply_run_formatting(r_nodes[min(i, len(r_nodes)-1)], run_formats[0])
                
            # Clear any remaining nodes
            if len(parts) < len(t_nodes):
                for t in t_nodes[len(parts):]:
                    t.text = ''
                    
        except Exception as e:
            logger.debug(f"Error distributing text: {e}")
            # Fallback: put all text in first node
            first_t = t_nodes[0]
            first_t.text = translated_text
            ensure_textnode_preserve_space(first_t)
            
            # Apply first run's formatting
            if r_nodes and run_formats:
                apply_run_formatting(r_nodes[0], run_formats[0])
                
            # Clear remaining nodes and enforce a single space between segments if the next segment starts without leading space
            for t in t_nodes[1:]:
                t.text = ''
        
        # Apply paragraph formatting
        if para_format and item.get('para'):
            apply_paragraph_formatting(item['para'], para_format)
        
        # Apply table cell formatting if present
        if cell_format and item.get('cell'):
            apply_table_cell_formatting(item['cell'], cell_format)
        
        # Apply Hindi-specific formatting if needed
        if target_language and target_language.lower().startswith('hi'):
            # Only apply Devanagari font to non-heading text
            if r_nodes and not is_heading_item(item, heading_style_names or []):
                apply_devanagari_to_first_run(r_nodes[0], font_name='Noto Sans Devanagari', min_pt=11)
            
    except Exception as e:
        logger.debug(f"apply_translation_to_item error for id {item.get('id')}: {e}")

def preserve_images(input_path: str, output_path: str):
    """Ensure images are preserved when copying the document"""
    try:
        shutil.copy(input_path, output_path)
        return True
    except Exception as e:
        logger.error(f"Failed to preserve images: {e}")
        return False

def translate_docx_advanced(input_path: str, output_path: str,
                            target_language: str = 'hi',
                            engine: str = 'groq',
                            tone: str = 'professional',
                            progress_callback=None,
                            max_chars_per_chunk: int = 6000,
                            max_total_chars: Optional[int] = 5000) -> Optional[Dict[str, int]]:
    logger.info(f"Translating {input_path} -> {output_path} ({target_language}, {engine})")
    
    if not preserve_images(input_path, output_path):
        return None
    
    try:
        available_keys = check_api_keys()
        if not available_keys.get(engine.lower(), False):
            raise RuntimeError(f"No API key available for {engine}.")
    except Exception as e:
        logger.error(f"API key check failed: {e}")
        return None

    doc = Document(output_path)
    items = collect_text_items_all_parts(doc)
    # Compute heading styles once and reuse
    heading_style_names = detect_heading_styles(doc)

    # Apply global character limit across concatenated item texts
    limited_items: List[Dict[str, Any]] = []
    limited_ids: set = set()
    if max_total_chars is not None and max_total_chars >= 0:
        consumed = 0
        for it in items:
            if consumed >= max_total_chars:
                break
            txt = (it.get('text') or '')
            remain = max_total_chars - consumed
            if len(txt) <= remain:
                limited_items.append(it)
                limited_ids.add(it.get('id'))
                consumed += len(txt)
            else:
                # Truncate this item and stop
                clone = {k: (v.copy() if isinstance(v, list) else v) for k, v in it.items()}
                clone['text'] = txt[:remain]
                limited_items.append(clone)
                limited_ids.add(it.get('id'))
                consumed += remain
                break
    else:
        limited_items = items
        limited_ids = {it.get('id') for it in items}

    # Clear all text for items beyond the limit (not in limited_ids)
    if limited_items is not items:
        for it in items:
            if it.get('id') not in limited_ids:
                for t in (it.get('t_nodes') or []):
                    try:
                        t.text = ''
                    except Exception:
                        continue

    total_items = len(limited_items)
    logger.info(f"Collected {len(items)} items; after char limit -> {total_items} items for translation.")
    
    if total_items > 0 and logger.isEnabledFor(logging.DEBUG):
        preview = [(it['id'], (it['text'] or '')[:120]) for it in limited_items[:10]]
        logger.debug(f"Items preview (first 10): {preview}")

    if total_items == 0:
        logger.info("No translatable items detected; returning early with saved copy.")
        return {"total_paragraphs": 0}

    try:
        translated_map = translate_text_json(limited_items, engine=engine, target_language=target_language, temperature=0.5, max_chars_per_chunk=max_chars_per_chunk, doc=doc)
    except Exception as e:
        logger.error(f"Translation failed: {e}. Falling back to original texts for all items.")
        translated_map = {it['id']: it['text'] for it in limited_items}

    processed = 0
    id_to_item = {it['id']: it for it in items}
    for it in limited_items:
        id_ = it['id']
        translated_text = translated_map.get(id_, it['text'])
        item = id_to_item.get(id_)
        if not item:
            continue

        try:
            lang_val = None
            try:
                lang_nodes = item['para'].xpath('.//w:rPr/w:lang', namespaces=_NS)
                if lang_nodes:
                    try:
                        lang_val = lang_nodes[0].get(qn('w:val'))
                    except Exception:
                        lang_val = None
            except Exception:
                lang_val = None

            if is_rtl_language(lang_val) or is_arabic_text(translated_text):
                _set_xml_para_rtl(item['para'], True)
            else:
                _set_xml_para_rtl(item['para'], False)
        except Exception as e:
            logger.debug(f"RTL detection/error for item {id_}: {e}")

        apply_translation_to_item(item, translated_text, target_language=target_language, heading_style_names=heading_style_names)

        processed += 1
        if progress_callback:
            try:
                progress_callback(processed, total_items)
            except Exception:
                pass

    # Apply Noto Sans Devanagari only if target language is Hindi
    if target_language and target_language.lower().startswith('hi'):
        try:
            logger.info("Applying Noto Sans Devanagari font across document (Hindi target).")
            apply_noto_sans_devanagari(doc, force=True, min_pt=11)
        except Exception as e:
            logger.warning(f"Failed to apply Noto Sans Devanagari across document: {e}")

    try:
        tmp_out = output_path + ".tmp"
        doc.save(tmp_out)
        os.replace(tmp_out, output_path)
        logger.info(f"Successfully saved translated doc to {output_path} (items processed: {processed})")
    except Exception as e:
        logger.exception(f"Failed saving translated document: {e}")
        return None

    return {"total_paragraphs": processed}
