# Copyright 2024 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

from abc import ABC, abstractmethod

from .etl_base import EtlBase


class CreateOmopDb(EtlBase, ABC):
    """
    Class that creates the CDM folder structure that holds the raw queries, Usagi CSV's and custom concept CSV's.
    """

    def __init__(
        self,
        target_dialect: str,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.target_dialect = target_dialect

    @abstractmethod
    def run(self) -> None:
        """Create OMOP tables in the database and define indexes/partitions/clusterings"""
        pass
