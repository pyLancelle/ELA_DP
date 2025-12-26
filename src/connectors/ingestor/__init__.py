"""
Generic Data Ingestor
---------------------
Unified interface for ingesting data from multiple services into BigQuery.
"""

from src.connectors.ingestor.base import IngestorAdapter, IngestResult

__all__ = ["IngestorAdapter", "IngestResult"]
