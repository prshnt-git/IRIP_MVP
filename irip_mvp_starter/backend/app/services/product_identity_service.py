from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from difflib import SequenceMatcher


@dataclass(frozen=True)
class ProductIdentity:
    raw_name: str
    brand: str | None
    normalized_brand: str | None
    normalized_model_key: str
    canonical_product_key: str
    product_id: str
    variant_key: str | None = None
    confidence: float = 1.0
    match_reason: str = "deterministic"


class ProductIdentityService:
    """Canonical product identity resolver for IRIP.

    This is the foundation for:
    - Own product catalog: TECNO / Infinix / itel
    - Future competitor catalog: Redmi / realme / Samsung / vivo / OPPO etc.
    - Review import product matching
    - Duplicate prevention
    - Stable benchmark mapping
    """

    BRAND_ALIASES: dict[str, str] = {
        "tecno": "tecno",
        "techno": "tecno",
        "infinix": "infinix",
        "itel": "itel",
        "i tel": "itel",
        "xiaomi": "xiaomi",
        "mi": "xiaomi",
        "redmi": "redmi",
        "poco": "poco",
        "realme": "realme",
        "samsung": "samsung",
        "vivo": "vivo",
        "iqoo": "iqoo",
        "i qoo": "iqoo",
        "oppo": "oppo",
        "oneplus": "oneplus",
        "one plus": "oneplus",
        "motorola": "motorola",
        "moto": "motorola",
        "nothing": "nothing",
        "honor": "honor",
        "lava": "lava",
        "nokia": "nokia",
        "hmd": "hmd",
    }

    # Compact brand forms that often arrive without spacing.
    COMPACT_BRAND_PREFIXES: dict[str, str] = {
        "tecno": "tecno",
        "techno": "tecno",
        "infinix": "infinix",
        "itel": "itel",
        "redmi": "redmi",
        "realme": "realme",
        "samsung": "samsung",
        "vivo": "vivo",
        "oppo": "oppo",
        "oneplus": "oneplus",
        "poco": "poco",
        "moto": "motorola",
        "motorola": "motorola",
    }

    OWN_BRANDS = {"tecno", "infinix", "itel"}

    NOISE_TOKENS = {
        "mobile",
        "phone",
        "smartphone",
        "dual",
        "sim",
        "india",
        "indian",
        "edition",
        "new",
        "latest",
        "with",
        "and",
        "the",
        "only",
        "offer",
        "sale",
        "online",
        "unlocked",
        "android",
    }

    COLOR_WORDS = {
        "black",
        "white",
        "blue",
        "green",
        "gold",
        "silver",
        "gray",
        "grey",
        "purple",
        "orange",
        "yellow",
        "pink",
        "red",
        "cyan",
        "mint",
        "cream",
        "titanium",
        "graphite",
        "charcoal",
        "midnight",
        "sunset",
        "ocean",
        "forest",
        "glacier",
        "starry",
    }

    NETWORK_TOKENS = {"5g", "4g", "lte"}

    SERIES_COMPACT_RULES: list[tuple[str, str]] = [
        (r"\bpova\s*(\d+)", r"pova \1"),
        (r"\bspark\s*(\d+)", r"spark \1"),
        (r"\bpop\s*(\d+)", r"pop \1"),
        (r"\bnote\s*(\d+)", r"note \1"),
        (r"\bhot\s*(\d+)", r"hot \1"),
        (r"\bzero\s*(\d+)", r"zero \1"),
        (r"\bsmart\s*(\d+)", r"smart \1"),
        (r"\ba\s*(\d+)", r"a \1"),
        (r"\bs\s*(\d+)", r"s \1"),
        (r"\bgt\s*(\d+)", r"gt \1"),
        (r"\bnarzo\s*(\d+)", r"narzo \1"),
    ]

    def normalize_text(self, value: str | None) -> str:
        if not value:
            return ""

        text = unicodedata.normalize("NFKC", str(value))
        text = text.replace("+", " plus ")
        text = text.replace("&", " and ")
        text = text.lower()

        # Preserve 5G/4G as compact tokens before generic letter-number spacing.
        text = re.sub(r"\b5\s*g\b", "5g", text)
        text = re.sub(r"\b4\s*g\b", "4g", text)

        # Expand compact brand prefixes: RedmiNote -> Redmi Note, TecnoPova -> Tecno Pova.
        for compact_brand in sorted(self.COMPACT_BRAND_PREFIXES, key=len, reverse=True):
            text = re.sub(
                rf"\b{compact_brand}(?=[a-z0-9])",
                f"{compact_brand} ",
                text,
                flags=re.IGNORECASE,
            )

        # Expand compact series names: Note50x -> Note 50x, Pova7 -> Pova 7.
        for pattern, repl in self.SERIES_COMPACT_RULES:
            text = re.sub(pattern, repl, text, flags=re.IGNORECASE)

        # Carefully split letters/numbers but keep 5g/4g, RAM/storage tokens compact.
        text = re.sub(r"([a-z])(\d)", r"\1 \2", text)
        text = re.sub(r"(\d)([a-z])", r"\1 \2", text)

        # Re-join known smartphone tokens after spacing.
        text = re.sub(r"\b5\s*g\b", "5g", text)
        text = re.sub(r"\b4\s*g\b", "4g", text)
        text = re.sub(r"\b(\d+)\s*gb\b", r"\1gb", text)
        text = re.sub(r"\b(\d+)\s*tb\b", r"\1tb", text)
        text = re.sub(r"\b(\d+)\s*mah\b", r"\1mah", text)
        text = re.sub(r"\b(\d+)\s*w\b", r"\1w", text)
        text = re.sub(r"\bpro\s*plus\b", "pro plus", text)
        text = re.sub(r"\bnote\s*(\d+)\s*x\b", r"note \1x", text)
        text = re.sub(r"\ba\s*(\d+)\b", r"a\1", text)

        text = re.sub(r"[^a-z0-9]+", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def normalize_brand(self, value: str | None) -> str | None:
        text = self.normalize_text(value)
        if not text:
            return None

        if text in self.BRAND_ALIASES:
            return self.BRAND_ALIASES[text]

        for size in (2, 1):
            prefix = " ".join(text.split()[:size])
            if prefix in self.BRAND_ALIASES:
                return self.BRAND_ALIASES[prefix]

        return text.replace(" ", "_")

    def infer_brand_from_name(self, product_name: str | None) -> str | None:
        raw = str(product_name or "").strip().lower()
        text = self.normalize_text(product_name)

        if not text:
            return None

        tokens = text.split()
        for size in (2, 1):
            candidate = " ".join(tokens[:size])
            if candidate in self.BRAND_ALIASES:
                return self.BRAND_ALIASES[candidate]

        # Fallback for badly compact names before normalization.
        compact = re.sub(r"[^a-z0-9]+", "", raw)
        for compact_brand, canonical in sorted(self.COMPACT_BRAND_PREFIXES.items(), key=lambda item: len(item[0]), reverse=True):
            if compact.startswith(compact_brand):
                return canonical

        return None

    def strip_brand_from_name(self, product_name: str, brand: str | None) -> str:
        text = self.normalize_text(product_name)
        normalized_brand = self.normalize_brand(brand) or self.infer_brand_from_name(product_name)

        if not normalized_brand:
            return text

        brand_aliases = [alias for alias, canonical in self.BRAND_ALIASES.items() if canonical == normalized_brand]
        brand_aliases = sorted(brand_aliases, key=len, reverse=True)

        for alias in brand_aliases:
            alias_norm = self.normalize_text(alias)
            if text == alias_norm:
                return ""
            if text.startswith(alias_norm + " "):
                return text[len(alias_norm):].strip()

        return text

    def extract_variant_key(self, product_name: str | None) -> str | None:
        text = self.normalize_text(product_name)
        if not text:
            return None

        # Common Indian e-commerce formats:
        # 8GB 256GB, 8 GB RAM | 256 GB ROM, 8/256, 8+256.
        ram = None
        storage = None

        ram_match = re.search(r"\b(\d{1,2})gb\s*(?:ram)?\b", text)
        if ram_match:
            ram = f"{ram_match.group(1)}gb"

        storage_matches = re.findall(r"\b(32gb|64gb|128gb|256gb|512gb|1tb|2tb)\b", text)
        if storage_matches:
            storage = storage_matches[-1]

        slash_match = re.search(r"\b(\d{1,2})\s*[/+]\s*(32|64|128|256|512)\b", text)
        if slash_match:
            ram = f"{slash_match.group(1)}gb"
            storage = f"{slash_match.group(2)}gb"

        parts: list[str] = []
        if ram:
            parts.append(f"{ram}_ram")
        if storage and storage != ram:
            parts.append(f"{storage}_storage")

        return "_".join(parts) if parts else None

    def model_key(self, product_name: str | None, brand: str | None = None) -> str:
        if not product_name:
            return "unknown_model"

        text = self.strip_brand_from_name(product_name, brand)
        tokens = []

        for token in text.split():
            if token in self.NOISE_TOKENS:
                continue
            if token in self.COLOR_WORDS:
                continue
            if re.fullmatch(r"\d{1,2}gb", token):
                continue
            if re.fullmatch(r"(32gb|64gb|128gb|256gb|512gb|1tb|2tb)", token):
                continue
            if re.fullmatch(r"\d+mah", token):
                continue
            if re.fullmatch(r"\d+w", token):
                continue
            tokens.append(token)

        if not tokens:
            return "unknown_model"

        return "_".join(tokens)

    def product_id_from_parts(
        self,
        brand: str | None,
        product_name: str,
        model_name: str | None = None,
    ) -> str:
        normalized_brand = self.normalize_brand(brand) or self.infer_brand_from_name(product_name)
        model_source = model_name or product_name
        model = self.model_key(model_source, normalized_brand)

        if normalized_brand:
            return self.slugify(f"{normalized_brand}_{model}")

        return self.slugify(model)

    def canonical_product_key(
        self,
        brand: str | None,
        product_name: str,
        model_name: str | None = None,
    ) -> str:
        normalized_brand = self.normalize_brand(brand) or self.infer_brand_from_name(product_name) or "unknown_brand"
        model = self.model_key(model_name or product_name, normalized_brand)
        return f"{normalized_brand}|{model}"

    def build_identity(
        self,
        product_name: str,
        brand: str | None = None,
        model_name: str | None = None,
    ) -> ProductIdentity:
        normalized_brand = self.normalize_brand(brand) or self.infer_brand_from_name(product_name)
        model = self.model_key(model_name or product_name, normalized_brand)
        canonical_key = self.canonical_product_key(
            brand=normalized_brand,
            product_name=product_name,
            model_name=model_name,
        )
        product_id = self.product_id_from_parts(
            brand=normalized_brand,
            product_name=product_name,
            model_name=model_name,
        )
        variant_key = self.extract_variant_key(product_name)

        return ProductIdentity(
            raw_name=product_name,
            brand=brand,
            normalized_brand=normalized_brand,
            normalized_model_key=model,
            canonical_product_key=canonical_key,
            product_id=product_id,
            variant_key=variant_key,
            confidence=1.0,
            match_reason="deterministic_normalization",
        )

    def compare_identity(self, left_name: str, right_name: str, left_brand: str | None = None, right_brand: str | None = None) -> dict:
        left = self.build_identity(left_name, left_brand)
        right = self.build_identity(right_name, right_brand)

        same_canonical = left.canonical_product_key == right.canonical_product_key
        same_model = left.normalized_model_key == right.normalized_model_key
        same_brand = left.normalized_brand == right.normalized_brand

        score = self.similarity(left.canonical_product_key, right.canonical_product_key)
        if same_canonical:
            score = 1.0
        elif same_model and same_brand:
            score = max(score, 0.96)
        elif same_model:
            score = max(score, 0.82)

        return {
            "left": left,
            "right": right,
            "same_canonical_product": same_canonical,
            "same_brand": same_brand,
            "same_model": same_model,
            "similarity": round(score, 4),
            "match_decision": "same_product" if score >= 0.94 else "possible_match" if score >= 0.82 else "different_product",
        }

    def similarity(self, left: str | None, right: str | None) -> float:
        left_norm = self.normalize_text(left)
        right_norm = self.normalize_text(right)

        if not left_norm or not right_norm:
            return 0.0

        if left_norm == right_norm:
            return 1.0

        return round(SequenceMatcher(None, left_norm, right_norm).ratio(), 4)

    def is_own_brand(self, brand: str | None) -> bool:
        normalized = self.normalize_brand(brand)
        return bool(normalized and normalized in self.OWN_BRANDS)

    def slugify(self, value: str | None) -> str:
        if not value:
            return "unknown"

        text = unicodedata.normalize("NFKC", str(value))
        text = text.replace("+", " plus ")
        text = text.replace("&", " and ")
        text = text.lower()
        text = re.sub(r"[^a-z0-9]+", "_", text)
        text = re.sub(r"_+", "_", text).strip("_")

        # Safety pass for common smartphone tokens.
        text = text.replace("_5_g", "_5g").replace("_4_g", "_4g")
        text = re.sub(r"_a_(\d+)", r"_a\1", text)
        text = re.sub(r"_s_(\d+)", r"_s\1", text)
        text = re.sub(r"_(\d+)_x\b", r"_\1x", text)
        text = re.sub(r"_(\d+)_x_", r"_\1x_", text)

        return text or "unknown"

