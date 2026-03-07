"""Centralized i18n helper exposing a dynamic gettext wrapper.

Other modules should import `_` from this module.
Call `set_language(lang)` to change the active language at runtime.
"""

import gettext
from collections.abc import Callable
from pathlib import Path

# underlying gettext function (will be rebound by set_language)
_current_gettext: Callable[[str], str] = gettext.gettext


def _gettext(message: str) -> str:
    """Return translated message using the currently configured translator."""
    return _current_gettext(message)


def set_language(lang: str, locale_dir: Path) -> None:
    """Set active language. If translation files aren't available, fallback to builtin gettext."""
    global _current_gettext  # noqa: PLW0603
    try:
        translations = gettext.translation("messages", locale_dir, languages=[lang])
        _current_gettext = translations.gettext
    except Exception:  # noqa: BLE001
        # fallback to no-op gettext
        _current_gettext = gettext.gettext


# default language
set_language("en", locale_dir=Path(__file__) / "locales")

_ = _gettext
