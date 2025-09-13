"""
rtl_utils.py
Helpers for RTL handling and XML paragraph bidi/textDirection manipulation.
Self-contained so it can be imported by translator.py.
"""
from typing import Optional
import logging
import re
from docx.oxml import OxmlElement
from docx.oxml.ns import qn, nsmap

logger = logging.getLogger(__name__)

XML_SPACE = '{http://www.w3.org/XML/1998/namespace}space'
_WPS = 'http://schemas.microsoft.com/office/word/2010/wordprocessingShape'
_VML = 'urn:schemas-microsoft-com:vml'
_MATH_NS = 'http://schemas.openxmlformats.org/officeDocument/2006/math'
_NS = {'w': nsmap['w'], 'wps': _WPS, 'v': _VML, 'm': _MATH_NS}

def ensure_textnode_preserve_space(t_elem) -> None:
    """Ensure w:t has xml:space='preserve' where appropriate."""
    try:
        t_elem.set(XML_SPACE, 'preserve')
    except Exception:
        # best-effort only
        pass

def ensure_paragraph_line_spacing(p, min_line_twips=360) -> None:
    """Ensure a paragraph has some minimal line spacing (twips) to avoid bidi layout issues."""
    try:
        pPr = p.find(qn('w:pPr'))
        if pPr is None:
            pPr = OxmlElement('w:pPr')
            p.insert(0, pPr)

        spacing = pPr.find(qn('w:spacing'))
        if spacing is None:
            spacing = OxmlElement('w:spacing')
            pPr.append(spacing)

        spacing.set(qn('w:line'), str(min_line_twips))
        spacing.set(qn('w:lineRule'), 'auto')
    except Exception:
        pass

def _set_xml_para_rtl(p, rtl=True) -> None:
    """
    Set paragraph xml properties for RTL (bidi + textDirection).
    If rtl=False, attempt to remove bidi/textDirection settings.
    """
    try:
        pPr = p.find(qn('w:pPr'))
        if pPr is None:
            pPr = OxmlElement('w:pPr')
            p.insert(0, pPr)
        bidi_tag = pPr.find(qn('w:bidi'))
        textDirection = pPr.find(qn('w:textDirection'))
        if rtl:
            if bidi_tag is None:
                bidi_tag = OxmlElement('w:bidi')
                pPr.append(bidi_tag)
            bidi_tag.set(qn('w:val'), "1")
            if textDirection is None:
                textDirection = OxmlElement('w:textDirection')
                pPr.append(textDirection)
            textDirection.set(qn('w:val'), "rtl")
            ensure_paragraph_line_spacing(p, min_line_twips=360)
        else:
            if bidi_tag is not None:
                try:
                    pPr.remove(bidi_tag)
                except Exception:
                    pass
            if textDirection is not None:
                try:
                    pPr.remove(textDirection)
                except Exception:
                    pass
            ensure_paragraph_line_spacing(p, min_line_twips=360)
    except Exception as e:
        logger.debug(f"_set_xml_para_rtl error: {e}")

def is_rtl_language(lang_code: Optional[str]) -> bool:
    """Return True for common RTL language codes (accepts 'ar', 'fa', 'he', etc.)."""
    if not lang_code:
        return False
    rtl = {'ar', 'arc', 'dv', 'fa', 'ha', 'he', 'khw', 'ks', 'ku', 'ps', 'ur', 'yi'}
    try:
        return lang_code.lower().split('-')[0] in rtl
    except Exception:
        return False

def is_arabic_text(text: str) -> bool:
    """
    Heuristic to detect presence of Arabic-script characters strong enough
    to consider a paragraph 'Arabic' (useful when lang metadata is absent).
    """
    if not text:
        return False
    arabic_chars = sum(1 for c in text if '\u0600' <= c <= '\u06FF')
    alpha = sum(1 for c in text if c.isalpha())
    if alpha == 0:
        return False
    # require at least some portion to be Arabic letters
    return arabic_chars > 0 and (arabic_chars / alpha) > 0.15
