"""
Explore codes and lists.

This module provides functions to get details about codes and code lists stored by ONS.
Codes are used to provide a common definition when presenting statistics with related categories.
"""

from typing import Optional, List, Dict, Any

from .utils import (
    build_base_request,
    make_request,
    process_response,
    EMPTY,
)


def ons_codelists() -> Optional[List[str]]:
    """Get a list of all available code lists.

    Returns:
        List of code list IDs, or None if the request fails

    Examples:
        >>> import onspy
        >>> onspy.ons_codelists()
    """
    req = build_base_request(**{"code-lists": EMPTY})
    res = make_request(req, limit=80)
    if res is None:
        return None

    raw = process_response(res)

    # Extract IDs from items
    try:
        return [item["links"]["self"]["id"] for item in raw.get("items", [])]
    except (KeyError, TypeError):
        return []


def assert_valid_codeid(id: str) -> bool:
    """Check if a code list ID is valid.

    Args:
        id: Code list ID

    Returns:
        True if valid, raises ValueError otherwise
    """
    if id is None:
        raise ValueError("You must specify a 'code_id', see ons_codelists()")

    ids = ons_codelists()
    if ids is None:
        return False

    if id not in ids:
        raise ValueError(f"Invalid code_id '{id}'. See ons_codelists() for valid IDs.")

    return True


def ons_codelist(code_id: str = None) -> Optional[Dict[str, Any]]:
    """Get details for a specific code list.

    Args:
        code_id: Code list ID

    Returns:
        Dictionary with code list details, or None if the request fails

    Examples:
        >>> import onspy
        >>> onspy.ons_codelist(code_id="quarter")
    """
    if not assert_valid_codeid(code_id):
        return None

    req = build_base_request(**{"code-lists": code_id})
    res = make_request(req)
    if res is None:
        return None

    return process_response(res)


def ons_codelist_editions(code_id: str = None) -> Optional[List[Dict[str, Any]]]:
    """Get editions for a code list.

    Args:
        code_id: Code list ID

    Returns:
        List of editions, or None if the request fails

    Examples:
        >>> import onspy
        >>> onspy.ons_codelist_editions(code_id="quarter")
    """
    if not assert_valid_codeid(code_id):
        return None

    req = build_base_request(**{"code-lists": code_id, "editions": EMPTY})
    res = make_request(req)
    if res is None:
        return None

    raw = process_response(res)
    return raw.get("items", [])


def assert_valid_edition(code_id: str, edition: str) -> bool:
    """Check if an edition is valid for a code list.

    Args:
        code_id: Code list ID
        edition: Edition name

    Returns:
        True if valid, raises ValueError otherwise
    """
    if edition is None:
        raise ValueError("You must specify an 'edition', see ons_codelist_editions()")

    editions = ons_codelist_editions(code_id)
    if editions is None:
        return False

    edition_names = [e.get("edition", "") for e in editions]
    if edition not in edition_names:
        raise ValueError(
            f"Invalid edition '{edition}'. Valid editions are: {', '.join(edition_names)}"
        )

    return True


def ons_codelist_edition(
    code_id: str = None, edition: str = None
) -> Optional[Dict[str, Any]]:
    """Get details for a specific edition of a code list.

    Args:
        code_id: Code list ID
        edition: Edition name

    Returns:
        Dictionary with edition details, or None if the request fails

    Examples:
        >>> import onspy
        >>> onspy.ons_codelist_edition(code_id="quarter", edition="one-off")
    """
    if not assert_valid_codeid(code_id):
        return None

    if not assert_valid_edition(code_id, edition):
        return None

    req = build_base_request(**{"code-lists": code_id, "editions": edition})
    res = make_request(req)
    if res is None:
        return None

    return process_response(res)


def ons_codes(
    code_id: str = None, edition: str = None
) -> Optional[List[Dict[str, Any]]]:
    """Get codes for a specific edition of a code list.

    Args:
        code_id: Code list ID
        edition: Edition name

    Returns:
        List of codes, or None if the request fails

    Examples:
        >>> import onspy
        >>> onspy.ons_codes(code_id="quarter", edition="one-off")
    """
    if not assert_valid_codeid(code_id):
        return None

    if not assert_valid_edition(code_id, edition):
        return None

    req = build_base_request(
        **{"code-lists": code_id, "editions": edition, "codes": EMPTY}
    )
    res = make_request(req)
    if res is None:
        return None

    raw = process_response(res)
    return raw.get("items", [])


def assert_valid_code(code_id: str, edition: str, code: str) -> bool:
    """Check if a code is valid for an edition of a code list.

    Args:
        code_id: Code list ID
        edition: Edition name
        code: Code value

    Returns:
        True if valid, raises ValueError otherwise
    """
    if code is None:
        raise ValueError("You must specify a 'code', see ons_codes()")

    codes = ons_codes(code_id, edition)
    if codes is None:
        return False

    code_values = [c.get("code", "") for c in codes]
    if code not in code_values:
        raise ValueError(
            f"Invalid code '{code}'. Valid codes are: {', '.join(code_values)}"
        )

    return True


def ons_code(
    code_id: str = None, edition: str = None, code: str = None
) -> Optional[Dict[str, Any]]:
    """Get details for a specific code.

    Args:
        code_id: Code list ID
        edition: Edition name
        code: Code value

    Returns:
        Dictionary with code details, or None if the request fails

    Examples:
        >>> import onspy
        >>> onspy.ons_code(code_id="quarter", edition="one-off", code="q2")
    """
    if not assert_valid_codeid(code_id):
        return None

    if not assert_valid_edition(code_id, edition):
        return None

    if not assert_valid_code(code_id, edition, code):
        return None

    req = build_base_request(
        **{"code-lists": code_id, "editions": edition, "codes": code}
    )
    res = make_request(req)
    if res is None:
        return None

    return process_response(res)


def ons_code_dataset(
    code_id: str = None, edition: str = None, code: str = None
) -> Optional[List[Dict[str, Any]]]:
    """Get datasets that use a specific code.

    Args:
        code_id: Code list ID
        edition: Edition name
        code: Code value

    Returns:
        List of datasets, or None if the request fails

    Examples:
        >>> import onspy
        >>> onspy.ons_code_dataset(code_id="quarter", edition="one-off", code="q2")
    """
    if not assert_valid_codeid(code_id):
        return None

    if not assert_valid_edition(code_id, edition):
        return None

    if not assert_valid_code(code_id, edition, code):
        return None

    req = build_base_request(
        **{"code-lists": code_id, "editions": edition, "codes": code, "datasets": EMPTY}
    )

    res = make_request(req)
    if res is None:
        return None

    raw = process_response(res)
    return raw.get("items", [])
