import pandas as pd


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
