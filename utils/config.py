"""
utils/config.py

Pipeline configuration loader.

Reads config.yml from the project root and returns a typed PipelineConfig
dataclass.  Every pipeline script imports from here — defaults are defined
once, config values override them, and CLI flags override config.

Usage
-----
    from utils.config import load_config

    cfg = load_config()
    data_dir = args.dir or cfg.bnf_data_path
    out_dir  = args.out_dir or cfg.resolved_bnf_survey_dir()

Config file
-----------
    Copy config.example.yml → config.yml and fill in your local paths.
    config.yml is gitignored — it may contain absolute paths and should
    never be committed.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml

_CONFIG_PATH        = Path(__file__).resolve().parent.parent / "config.yml"
_OPENITI_CONFIG_PATH = Path(__file__).resolve().parent.parent / "openiti.yml"


# ---------------------------------------------------------------------------
# Nested config sections
# ---------------------------------------------------------------------------

@dataclass
class SurveyConfig:
    max_n:             int  = 4
    keep_abbrev_dots:  bool = True


@dataclass
class FieldBoilerplateConfig:
    """Boilerplate detection settings for a single DC field.

    mode
        "full"      — apply both doc_freq_pct AND max_repeats_per_doc.
                      Use for mixed-content fields (dc:description) where
                      boilerplate and meaningful content coexist; the
                      repeats criterion separates structural phrases
                      (appear once per record) from name fragments.
        "freq_only" — apply only doc_freq_pct.
                      Use for fields that are predominantly or entirely
                      boilerplate (dc:format, dc:rights, dc:source) where
                      the repeats criterion adds no discriminating power.
    """
    mode:                str   = "full"
    min_doc_freq_pct:    float = 5.0
    max_repeats_per_doc: float = 1.1


def _default_boilerplate_fields() -> dict[str, FieldBoilerplateConfig]:
    return {
        "description": FieldBoilerplateConfig(
            mode="full", min_doc_freq_pct=2.0, max_repeats_per_doc=1.1,
        ),
        "creator": FieldBoilerplateConfig(
            mode="freq_only", min_doc_freq_pct=10.0,
        ),
        "subject": FieldBoilerplateConfig(
            mode="freq_only", min_doc_freq_pct=15.0,
        ),
    }


@dataclass
class BoilerplateConfig:
    """Per-field boilerplate detection configuration.

    Each entry in ``fields`` specifies how to identify boilerplate n-grams
    for a specific DC field.  Fields not listed are not scanned.

    Example config.yml::

        boilerplate:
          fields:
            description:
              mode: full
              min_doc_freq_pct: 2.0
              max_repeats_per_doc: 1.1
            creator:
              mode: freq_only
              min_doc_freq_pct: 10.0
    """
    fields: dict[str, FieldBoilerplateConfig] = field(
        default_factory=_default_boilerplate_fields
    )


@dataclass
class ParsingConfig:
    overwrite_existing: bool = False


@dataclass
class SurfaceMatchConfig:
    fuzzy_threshold: float = 0.8


@dataclass
class EmbeddingConfig:
    model:      str = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
    batch_size: int = 32


@dataclass
class MatchingConfig:
    surface_form: SurfaceMatchConfig = field(default_factory=SurfaceMatchConfig)
    embedding:    EmbeddingConfig    = field(default_factory=EmbeddingConfig)


@dataclass
class ClusteringConfig:
    algorithm:        str = "hdbscan"
    min_cluster_size: int = 2


@dataclass
class WorldCatConfig:
    """HTTP settings for the WorldCat enrichment stage."""
    request_delay: float = 1.0   # seconds between requests (minimum 1.0)
    timeout:       int   = 12    # HTTP timeout in seconds


@dataclass
class OpenITICorpusConfig:
    """OpenITI corpus configuration.

    Read from openiti.yml (committed to the repo).  Controls the corpus
    version identifier used to name data/ output files and recorded in
    pipeline manifests, plus HTTP settings for the WorldCat enrichment stage.

    corpus_version
        The version string for the current OpenITI corpus snapshot
        (e.g. "corpus_2025_1_9").  This names the output files:
          data/openiti_parsed_<corpus_version>.json
          data/openiti_worldcat_<corpus_version>.json
        Update this field when downloading a new corpus release.
    """
    corpus_version: str          = ""
    worldcat:       WorldCatConfig = field(default_factory=WorldCatConfig)


# ---------------------------------------------------------------------------
# Top-level config
# ---------------------------------------------------------------------------

@dataclass
class PipelineConfig:
    """Full resolved pipeline configuration.

    All fields have sensible defaults so the pipeline runs without a
    config.yml (useful for tests).  Real runs should set at minimum
    bnf_data_path and openiti_data_path.
    """
    # Data sources
    bnf_data_path:      Optional[str] = None
    openiti_data_path:  Optional[str] = None

    # Output root — all pipeline artifacts live under here (gitignored)
    pipeline_out_dir:   str           = "outputs"

    # Survey output directory — defaults to <pipeline_out_dir>/bnf_survey
    # Set explicitly in config.yml to use an absolute path outside the repo.
    bnf_survey_dir:     Optional[str] = None

    # Stage configs
    survey:      SurveyConfig      = field(default_factory=SurveyConfig)
    boilerplate: BoilerplateConfig = field(default_factory=BoilerplateConfig)
    parsing:     ParsingConfig     = field(default_factory=ParsingConfig)
    matching:    MatchingConfig    = field(default_factory=MatchingConfig)
    clustering:  ClusteringConfig  = field(default_factory=ClusteringConfig)

    def resolved_bnf_survey_dir(self) -> str:
        """Return the BNF survey directory, resolving the default if not set."""
        if self.bnf_survey_dir:
            return self.bnf_survey_dir
        return str(Path(self.pipeline_out_dir) / "bnf_survey")

    def resolved_runs_dir(self) -> str:
        """Return the runs directory under pipeline_out_dir."""
        return str(Path(self.pipeline_out_dir) / "runs")


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

def _parse_field_boilerplate(raw_fields: dict) -> dict[str, FieldBoilerplateConfig]:
    """Parse the boilerplate.fields section of config.yml."""
    defaults = _default_boilerplate_fields()
    result: dict[str, FieldBoilerplateConfig] = {}
    for fname, fraw in raw_fields.items():
        if not isinstance(fraw, dict):
            fraw = {}
        default = defaults.get(fname, FieldBoilerplateConfig())
        result[fname] = FieldBoilerplateConfig(
            mode                = str(fraw.get("mode", default.mode)),
            min_doc_freq_pct    = float(fraw.get("min_doc_freq_pct", default.min_doc_freq_pct)),
            max_repeats_per_doc = float(fraw.get("max_repeats_per_doc", default.max_repeats_per_doc)),
        )
    return result or defaults


def load_config(path: str | None = None) -> PipelineConfig:
    """Load config.yml and return a PipelineConfig.

    Missing keys fall back to dataclass defaults.  The config file is
    optional — if absent, all defaults apply (useful in tests and CI).

    Parameters
    ----------
    path : str, optional
        Override the default config path (project root / config.yml).
    """
    config_path = Path(path) if path else _CONFIG_PATH
    raw: dict = {}
    if config_path.exists():
        with config_path.open(encoding="utf-8") as fh:
            raw = yaml.safe_load(fh) or {}

    survey_raw     = raw.get("survey", {})
    bp_raw         = raw.get("boilerplate", {})
    parsing_raw    = raw.get("parsing", {})
    matching_raw   = raw.get("matching", {})
    clustering_raw = raw.get("clustering", {})
    surface_raw    = matching_raw.get("surface_form", {})
    embedding_raw  = matching_raw.get("embedding", {})

    # Boilerplate: per-field structure supersedes flat legacy keys
    bp_fields_raw = bp_raw.get("fields", {})
    if bp_fields_raw:
        boilerplate_cfg = BoilerplateConfig(
            fields=_parse_field_boilerplate(bp_fields_raw)
        )
    else:
        # Legacy flat keys — migrate to description-only field config
        legacy_pct     = float(bp_raw.get("min_doc_freq_pct", 2.0))
        legacy_repeats = float(bp_raw.get("max_repeats_per_doc", 1.1))
        defaults = _default_boilerplate_fields()
        defaults["description"] = FieldBoilerplateConfig(
            mode="full",
            min_doc_freq_pct=legacy_pct,
            max_repeats_per_doc=legacy_repeats,
        )
        boilerplate_cfg = BoilerplateConfig(fields=defaults)

    return PipelineConfig(
        bnf_data_path     = raw.get("bnf_data_path") or None,
        openiti_data_path = raw.get("openiti_data_path") or None,
        pipeline_out_dir  = raw.get("pipeline_out_dir", "outputs"),
        bnf_survey_dir    = raw.get("bnf_survey_dir") or None,

        survey = SurveyConfig(
            max_n            = int(survey_raw.get("max_n", 4)),
            keep_abbrev_dots = bool(survey_raw.get("keep_abbrev_dots", True)),
        ),
        boilerplate = boilerplate_cfg,
        parsing = ParsingConfig(
            overwrite_existing = bool(parsing_raw.get("overwrite_existing", False)),
        ),
        matching = MatchingConfig(
            surface_form = SurfaceMatchConfig(
                fuzzy_threshold = float(surface_raw.get("fuzzy_threshold", 0.8)),
            ),
            embedding = EmbeddingConfig(
                model      = str(embedding_raw.get(
                    "model",
                    "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
                )),
                batch_size = int(embedding_raw.get("batch_size", 32)),
            ),
        ),
        clustering = ClusteringConfig(
            algorithm        = str(clustering_raw.get("algorithm", "hdbscan")),
            min_cluster_size = int(clustering_raw.get("min_cluster_size", 2)),
        ),
    )


def load_openiti_config(path: str | None = None) -> OpenITICorpusConfig:
    """Load openiti.yml and return an OpenITICorpusConfig.

    openiti.yml is committed to the repo (no local paths; safe to share).
    It controls the corpus version identifier used to name data/ files and
    recorded in pipeline manifests.

    Missing keys fall back to dataclass defaults.  The file is optional —
    if absent, defaults apply (corpus_version will be an empty string,
    which will cause the build/update scripts to raise a clear error).

    Parameters
    ----------
    path : str, optional
        Override the default openiti.yml path (project root / openiti.yml).
    """
    config_path = Path(path) if path else _OPENITI_CONFIG_PATH
    raw: dict = {}
    if config_path.exists():
        with config_path.open(encoding="utf-8") as fh:
            raw = yaml.safe_load(fh) or {}

    wc_raw = raw.get("worldcat", {})
    return OpenITICorpusConfig(
        corpus_version = str(raw.get("corpus_version", "")),
        worldcat       = WorldCatConfig(
            request_delay = float(wc_raw.get("request_delay", 1.0)),
            timeout       = int(wc_raw.get("timeout", 12)),
        ),
    )
