import logging

import pandas as pd
from googleapiclient.discovery import build

from services.google_auth_service import GoogleAuthService
logger = logging.getLogger(__name__)


class GA4FetchError(Exception):
    pass

class GoogleAnalytics4Service:
    def __init__(self, auth_domain):
        self.service = self._authenticate_ga4(auth_domain)

    @staticmethod
    def _authenticate_ga4(auth_domain):
        auth_service = GoogleAuthService(auth_domain)
        creds = auth_service.authenticate()
        return build("analyticsdata", "v1beta", credentials=creds)

    def fetch_data(self, ga_property_id, start_date, end_date, dimensions=None, metrics=None):
        # Setting default dimensions and metrics if they are not provided
        if dimensions is None:
            dimensions = [
                {"name": "landingPage"},
                {"name": "sessionDefaultChannelGroup"},
            ]
        if metrics is None:
            metrics = [
                {"name": "sessions"},
                {"name": "conversions"},
                {"name": "sessionConversionRate"},
                {"name": "totalRevenue"},
                {"name": "ecommercePurchases"},
                {"name": "purchaserConversionRate"},
                {"name": "bounceRate"},
                {"name": "averageSessionDuration"},
            ]

        property_id = f"properties/{ga_property_id}"

        logger.info(f"Fetching GA data from {start_date} to {end_date} for property {ga_property_id}")

        response = (
            self.service.properties()
            .runReport(
                property=property_id,
                body={
                    "date_ranges": [
                        {
                            "start_date": start_date.strftime("%Y-%m-%d"),
                            "end_date": end_date.strftime("%Y-%m-%d"),
                        }
                    ],
                    "dimensions": dimensions,
                    "metrics": metrics,
                },
            )
            .execute()
        )

        logger.info("GA data fetch completed")
        return self._flatten_data(response)

    def _flatten_data(self, response):
        # Check if the response has the necessary parts
        if "rows" not in response or "dimensionHeaders" not in response or "metricHeaders" not in response:
            return pd.DataFrame()  # Return an empty DataFrame if necessary parts are missing

        # Initialize empty lists for headers
        dimension_headers = [header["name"] for header in response["dimensionHeaders"]]
        metric_headers = [header["name"] for header in response["metricHeaders"]]

        # Flatten each row of the response
        flattened_data = []
        for row in response["rows"]:
            flattened_row = {}
            # Extract and add dimensions to the row
            for i, dimension in enumerate(row["dimensionValues"]):
                flattened_row[dimension_headers[i]] = dimension["value"]
            # Extract and add metrics to the row
            for i, metric in enumerate(row["metricValues"]):
                flattened_row[metric_headers[i]] = metric["value"]
            flattened_data.append(flattened_row)

        # Convert the flattened data into a DataFrame
        return pd.DataFrame(flattened_data)