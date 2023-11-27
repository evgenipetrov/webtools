import pandas as pd
import logging
from django.core.exceptions import FieldError

logger = logging.getLogger(__name__)


class DataframeService:
    @staticmethod
    def merge_keys(current_data, new_data, current_data_key, new_data_key):
        # Ensure key columns are of the same type
        new_data[new_data_key] = new_data[new_data_key].astype(str)
        current_data[current_data_key] = current_data[current_data_key].astype(str)

        # Identify and append new key values
        new_keys = new_data[~new_data[new_data_key].isin(current_data[current_data_key])][new_data_key]
        current_data = pd.concat([current_data, new_keys.to_frame(name=current_data_key)], ignore_index=True)

        return current_data

    @staticmethod
    def merge_data(current_data, new_data, current_data_key, new_data_key, current_data_column, new_data_column):
        # Perform a left join
        merged_data = current_data.merge(new_data, left_on=current_data_key, right_on=new_data_key, how="left")

        # Update only missing values in master_data_column
        current_data[current_data_column] = current_data[current_data_column].combine_first(merged_data[new_data_column])

        return current_data

    @staticmethod
    def get_unique_column_values(df, column_name):
        unique_values = df[column_name].unique()
        return unique_values

    @staticmethod
    def queryset_to_dataframe(queryset):
        """
        Converts a Django QuerySet to a pandas DataFrame.

        Args:
        - queryset: A Django QuerySet object

        Returns:
        - A pandas DataFrame containing the data from the QuerySet.

        Raises:
        - FieldError: If the QuerySet contains fields that are not compatible with DataFrame.
        """
        try:
            # Extracting the values from the queryset
            values = queryset.values()
            # Creating a DataFrame from the values
            df = pd.DataFrame(list(values))
            return df
        except FieldError as e:
            raise FieldError(f"Error converting QuerySet to DataFrame: {e}")
