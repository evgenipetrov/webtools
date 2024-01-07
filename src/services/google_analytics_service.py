import logging

import pandas as pd
from googleapiclient.discovery import build

from services.google_auth_service import GoogleAuthService

logger = logging.getLogger(__name__)


class GA4FetchError(Exception):
    pass


class GoogleAnalytics4Service:
    def __init__(self, auth_email):
        try:
            self.service = self._authenticate_ga4(auth_email)
            logger.info("Successfully authenticated Google Analytics 4 service.")
        except Exception as e:
            logger.error(f"Failed to authenticate Google Analytics 4: {e}")
            raise GA4FetchError("Authentication failed") from e

    @staticmethod
    def _authenticate_ga4(auth_email):
        auth_service = GoogleAuthService(auth_email)
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

        # Log the dimensions and metrics being used for the request
        dimension_names = [dim["name"] for dim in dimensions]
        metric_names = [metric["name"] for metric in metrics]
        logger.info(f"Fetching GA4 data for property {ga_property_id} from {start_date} to {end_date} with dimensions {dimension_names} and metrics {metric_names}")

        property_id = f"properties/{ga_property_id}"

        try:
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
            logger.info("Data fetched successfully.")
            return self._flatten_data(response)
        except Exception as e:
            logger.error(f"Failed to fetch GA4 data: {e}")
            raise GA4FetchError("Data fetch failed") from e

    @staticmethod
    def _flatten_data(response):
        # Check if the response has the necessary parts
        if "rows" not in response or "dimensionHeaders" not in response or "metricHeaders" not in response:
            logger.warning("Received incomplete data response")
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
