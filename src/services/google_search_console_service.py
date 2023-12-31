import datetime
import logging

import pandas as pd
import requests.exceptions
from googleapiclient.discovery import build
from tenacity import retry, retry_if_exception_type, stop_after_attempt
from tenacity import wait_exponential

from services.google_auth_service import GoogleAuthService

logger = logging.getLogger(__name__)


class GSCFetchError(Exception):
    pass


class GoogleSearchConsoleService:
    ROW_LIMIT = 25000

    def __init__(self, auth_email):
        try:
            self.service = self._authenticate_gsc(auth_email)
            logger.info("Successfully authenticated Google Search Console service.")
        except Exception as e:
            logger.error(f"Failed to authenticate Google Search Console: {e}")
            raise GSCFetchError("Authentication failed") from e

    @staticmethod
    def _create_request(
        start_date,
        end_date,
        dimensions,
        start_row,
        dimension_filter_groups=None,
        aggregation_type=None,
        data_type=None,
    ):
        request = {
            "startDate": start_date,
            "endDate": end_date,
            "dimensions": dimensions,
            "startRow": start_row,
            "rowLimit": GoogleSearchConsoleService.ROW_LIMIT,
        }

        if dimension_filter_groups:
            request["dimensionFilterGroups"] = dimension_filter_groups
        if aggregation_type:
            request["aggregationType"] = aggregation_type
        if data_type:
            request["type"] = data_type

        return request

    @staticmethod
    def _authenticate_gsc(auth_email):
        auth_service = GoogleAuthService(auth_email)
        creds = auth_service.authenticate()
        return build("webmasters", "v3", credentials=creds)

    @retry(
        stop=stop_after_attempt(10),
        wait=wait_exponential(multiplier=1, max=60),
        retry=retry_if_exception_type(requests.exceptions.ReadTimeout),
    )
    def execute_request(self, site_url, request_body) -> list:
        try:
            response = self.service.searchanalytics().query(siteUrl=site_url, body=request_body).execute()
            logger.info("Search Analytics query executed successfully.")
            return response.get("rows", [])
        except Exception as e:
            logger.error(f"Error while executing Search Analytics query: {e}")
            raise GSCFetchError(f"Error while fetching GSC data: {e}")

    @staticmethod
    def _flatten_entry(dimensions, entry):
        flat_entry = {}
        for dim, value in zip(dimensions, entry["keys"]):
            flat_entry[dim] = value
        flat_entry.update(
            {
                "clicks": entry["clicks"],
                "impressions": entry["impressions"],
                "ctr": round(entry["ctr"], 2),
                "position": round(entry["position"], 1),
            }
        )
        return flat_entry

    def fetch_data(self, site_url, start_date: datetime.date, end_date: datetime.date, dimensions) -> pd.DataFrame:
        all_data = []
        start_row = 0

        while True:
            request_body = self._create_request(
                start_date.strftime("%Y-%m-%d"),
                end_date.strftime("%Y-%m-%d"),
                dimensions,
                start_row,
            )
            try:
                logger.info(f"Fetching GSC data from {start_date} to {end_date} for {site_url}, dimensions {dimensions}, start row {start_row}")
                data = self.execute_request(site_url, request_body)
            except GSCFetchError as e:
                logger.error(str(e))
                # If the error is 'user does not have access', return an empty DataFrame
                if "HttpError 403" in str(e):
                    logger.error("User does not have access.")
                    return pd.DataFrame()
                continue

            if not data:
                break

            flattened_data = [self._flatten_entry(dimensions, entry) for entry in data]
            all_data.extend(flattened_data)

            start_row += len(data)

        logger.info(f"Fetched {len(all_data)} rows of GSC data")
        df = pd.DataFrame(all_data)
        required_columns = {"clicks", "impressions", "ctr", "position"}

        # Add missing columns with NaN values
        missing_columns = required_columns - set(df.columns)
        for column in missing_columns:
            df[column] = float("NaN")

        # Now, it's safe to cast types because all required columns are present
        return df.astype(
            {
                "clicks": "int64",
                "impressions": "int64",
                "ctr": "float64",
                "position": "float64",
            }
        )
