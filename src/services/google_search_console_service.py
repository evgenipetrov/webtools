import datetime

import pandas as pd
import requests.exceptions
from googleapiclient.discovery import build
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from services.google_auth_service import GoogleAuthService

import logging

logger = logging.getLogger(__name__)


class GSCFetchError(Exception):
    pass


class GoogleSearchConsoleService:
    ROW_LIMIT = 25000

    def __init__(self, auth_domain):
        self.service = self._authenticate_gsc(auth_domain)

    def _create_request(
        self,
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
    def _authenticate_gsc(auth_domain):
        auth_service = GoogleAuthService(auth_domain)
        creds = auth_service.authenticate()
        return build("webmasters", "v3", credentials=creds)

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),  # 2s, 4s, 8s, ... max 30s between retries
        retry=retry_if_exception_type(requests.exceptions.ReadTimeout),  # Retry only on timeouts
    )
    def execute_request(self, site_url, request_body) -> list:
        try:
            response = self.service.searchanalytics().query(siteUrl=site_url, body=request_body).execute()
            return response.get("rows", [])
        except Exception as e:
            raise GSCFetchError(f"Error while fetching GSC data: {e}")

    def _flatten_entry(self, dimensions, entry):
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
                continue

            if not data:
                break

            # Use the new method for flattening data
            flattened_data = [self._flatten_entry(dimensions, entry) for entry in data]
            all_data.extend(flattened_data)

            start_row += len(data)

        logger.info(f"Fetched {len(all_data)} rows of GSC data")

        # Convert list of flat data to pandas DataFrame
        df = pd.DataFrame(all_data)

        # Ensure data types
        df = df.astype(
            {
                "clicks": "int",
                "impressions": "int",
                "ctr": "float64",
                "position": "float64",
            }
        )

        return df
