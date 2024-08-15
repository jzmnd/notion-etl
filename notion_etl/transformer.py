from abc import ABC, abstractmethod

import pandas as pd


class BaseTransformer(ABC):
    """Bass class for data transformer using Pandas dataframes."""

    @abstractmethod
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Perform data transformation.

        Implement this method when creating custom transformers.
        """
        raise NotImplementedError
