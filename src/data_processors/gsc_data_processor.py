import logging

import pandas as pd
from core.models import Project
from core.models.gscpage import GscManager
from core.models.url import UrlManager
from core.models.website import WebsiteManager
from exports.googlesearchconsole_page_last_16m_export import GoogleSearchConsolePageLast16mExport
from exports.googlesearchconsole_page_last_1m_export import GoogleSearchConsolePageLast1mExport
from exports.googlesearchconsole_page_last_1m_previous_year_export import GoogleSearchConsolePageLast1mPreviousYearExport
from exports.googlesearchconsole_page_previous_1m_export import GoogleSearchConsolePagePrevious1mExport
from exports.googlesearchconsole_page_query_last_16m_export import GoogleSearchConsolePageQueryLast16mExport
from exports.googlesearchconsole_page_query_last_1m_export import GoogleSearchConsolePageQueryLast1mExport
from exports.googlesearchconsole_page_query_last_1m_previous_year_export import GoogleSearchConsolePageQueryLast1mPreviousYearExport
from exports.googlesearchconsole_page_query_previous_1m_export import GoogleSearchConsolePageQueryPrevious1mExport
from exports.googlesearchconsole_query_last_16m_export import GoogleSearchConsoleQueryLast16mExport
from exports.googlesearchconsole_query_last_1m_export import GoogleSearchConsoleQueryLast1mExport
from exports.googlesearchconsole_query_last_1m_previous_year_export import GoogleSearchConsoleQueryLast1mPreviousYearExport
from exports.googlesearchconsole_query_previous_1m_export import GoogleSearchConsoleQueryPrevious1mExport

logger = logging.getLogger(__name__)


def aggregate_gsc_data(df):
    return df.groupby('redirect_url').agg({
        'impressions': 'sum',
        'clicks': 'sum',
        'ctr': 'mean',
        'position': 'mean'
    })


def get_aggregated_data_for_url(agg_data, redirect_url, default_value=0):
    return agg_data.loc[redirect_url] if redirect_url in agg_data.index else {
        'impressions': default_value,
        'clicks': default_value,
        'ctr': default_value,
        'position': default_value
    }


class GscDataProcessor:
    def __init__(self, project: Project):
        self._project = project

        self._data = None

    def run(self):
        self.collect_data()
        self.process_data()
        self.store_data()

    def collect_data(self):
        googlesearchconsole_page_last_1m_export = GoogleSearchConsolePageLast1mExport(self._project)
        self.googlesearchconsole_page_last_1m_data = googlesearchconsole_page_last_1m_export.get_data()

        googlesearchconsole_page_last_16m_export = GoogleSearchConsolePageLast16mExport(self._project)
        self.googlesearchconsole_page_last_16m_data = googlesearchconsole_page_last_16m_export.get_data()

        googlesearchconsole_page_previous_1m_export = GoogleSearchConsolePagePrevious1mExport(self._project)
        self.googlesearchconsole_page_previous_1m_data = googlesearchconsole_page_previous_1m_export.get_data()

        googlesearchconsole_page_last_1m_previous_year_export = GoogleSearchConsolePageLast1mPreviousYearExport(self._project)
        self.googlesearchconsole_page_last_1m_previous_year_data = googlesearchconsole_page_last_1m_previous_year_export.get_data()

        googlesearchconsole_query_last_1m_export = GoogleSearchConsoleQueryLast1mExport(self._project)
        self.googlesearchconsole_query_last_1m_data = googlesearchconsole_query_last_1m_export.get_data()

        googlesearchconsole_query_last_16m_export = GoogleSearchConsoleQueryLast16mExport(self._project)
        self.googlesearchconsole_query_last_16m_data = googlesearchconsole_query_last_16m_export.get_data()

        googlesearchconsole_query_previous_1m_export = GoogleSearchConsoleQueryPrevious1mExport(self._project)
        self.googlesearchconsole_query_previous_1m_data = googlesearchconsole_query_previous_1m_export.get_data()

        googlesearchconsole_query_last_1m_previous_year_export = GoogleSearchConsoleQueryLast1mPreviousYearExport(self._project)
        self.googlesearchconsole_query_last_1m_previous_year_data = googlesearchconsole_query_last_1m_previous_year_export.get_data()

        googlesearchconsole_page_query_last_1m_export = GoogleSearchConsolePageQueryLast1mExport(self._project)
        self.googlesearchconsole_page_query_last_1m_data = googlesearchconsole_page_query_last_1m_export.get_data()

        googlesearchconsole_page_query_last_16m_export = GoogleSearchConsolePageQueryLast16mExport(self._project)
        self.googlesearchconsole_page_query_last_16m_data = googlesearchconsole_page_query_last_16m_export.get_data()

        googlesearchconsole_page_query_previous_1m_export = GoogleSearchConsolePageQueryPrevious1mExport(self._project)
        self.googlesearchconsole_page_query_previous_1m_data = googlesearchconsole_page_query_previous_1m_export.get_data()

        googlesearchconsole_page_query_last_1m_previous_year_export = GoogleSearchConsolePageQueryLast1mPreviousYearExport(self._project)
        self.googlesearchconsole_page_query_last_1m_previous_year_data = googlesearchconsole_page_query_last_1m_previous_year_export.get_data()

    def process_data(self):
        website = WebsiteManager.get_website_by_project(self._project)
        urls = UrlManager.get_urls_by_website(website)

        url_table = [{'full_address': url.full_address, 'status_code': url.status_code, 'redirect_url': url.redirect_url} for url in urls]

        df = pd.DataFrame(url_table)

        # Fill empty 'redirect_url' with 'full_address'
        df['redirect_url'] = df.apply(lambda row: row['full_address'] if pd.isna(row['redirect_url']) or row['redirect_url'] == '' else row['redirect_url'], axis=1)

        self._data = df

        # group data
        googlesearchconsole_page_last_1m_aggregated_data = aggregate_gsc_data(df.merge(self.googlesearchconsole_page_last_1m_data, left_on='full_address', right_on='page', how='left'))
        googlesearchconsole_page_last_1m_aggregated_data = googlesearchconsole_page_last_1m_aggregated_data.rename(columns=lambda x: x + '_last_1m' if x != 'redirect_url' else x)
        self._data = self._data.merge(
            googlesearchconsole_page_last_1m_aggregated_data,
            left_on='redirect_url',
            right_index=True,
            how='left',
        )

        googlesearchconsole_page_last_1m_previous_year_aggregated_data = aggregate_gsc_data(df.merge(self.googlesearchconsole_page_last_1m_previous_year_data, left_on='full_address', right_on='page', how='left'))
        googlesearchconsole_page_last_1m_previous_year_aggregated_data = googlesearchconsole_page_last_1m_previous_year_aggregated_data.rename(columns=lambda x: x + '_last_1m_previous_year' if x != 'redirect_url' else x)
        self._data = self._data.merge(
            googlesearchconsole_page_last_1m_previous_year_aggregated_data,
            left_on='redirect_url',
            right_index=True,
            how='left',
        )
        googlesearchconsole_page_last_16m_aggregated_data = aggregate_gsc_data(df.merge(self.googlesearchconsole_page_last_16m_data, left_on='full_address', right_on='page', how='left'))
        googlesearchconsole_page_last_16m_aggregated_data = googlesearchconsole_page_last_16m_aggregated_data.rename(columns=lambda x: x + 'last_16m' if x != 'redirect_url' else x)
        self._data = self._data.merge(
            googlesearchconsole_page_last_16m_aggregated_data,
            left_on='redirect_url',
            right_index=True,
            how='left',
        )
        googlesearchconsole_page_previous_1m_aggregated_data = aggregate_gsc_data(df.merge(self.googlesearchconsole_page_previous_1m_data, left_on='full_address', right_on='page', how='left'))
        googlesearchconsole_page_previous_1m_aggregated_data = googlesearchconsole_page_previous_1m_aggregated_data.rename(columns=lambda x: x + 'previous_1m' if x != 'redirect_url' else x)
        self._data = self._data.merge(
            googlesearchconsole_page_previous_1m_aggregated_data,
            left_on='redirect_url',
            right_index=True,
            how='left',
        )

        # googlesearchconsole_query_last_1m_aggregated_data = aggregate_gsc_data(df.merge(self.googlesearchconsole_query_last_1m_data, left_on='full_address', right_on='page', how='left'))
        # googlesearchconsole_query_last_16m_aggregated_data = aggregate_gsc_data(df.merge(self.googlesearchconsole_query_last_16m_data, left_on='full_address', right_on='page', how='left'))
        # googlesearchconsole_query_previous_1m_aggregated_data = aggregate_gsc_data(df.merge(self.googlesearchconsole_query_previous_1m_data, left_on='full_address', right_on='page', how='left'))
        # googlesearchconsole_query_last_1m_previous_year_aggregated_data = aggregate_gsc_data(df.merge(self.googlesearchconsole_query_last_1m_previous_year_data, left_on='full_address', right_on='page', how='left'))
        # googlesearchconsole_page_query_last_1m_aggregated_data = aggregate_gsc_data(df.merge(self.googlesearchconsole_page_query_last_1m_data, left_on='full_address', right_on='page', how='left'))
        # googlesearchconsole_page_query_last_16m_aggregated_data = aggregate_gsc_data(df.merge(self.googlesearchconsole_page_query_last_16m_data, left_on='full_address', right_on='page', how='left'))
        # googlesearchconsole_page_query_previous_1m_aggregated_data = aggregate_gsc_data(df.merge(self.googlesearchconsole_page_query_previous_1m_data, left_on='full_address', right_on='page', how='left'))
        # googlesearchconsole_page_query_last_1m_previous_year_aggregated_data = aggregate_gsc_data(df.merge(self.googlesearchconsole_page_query_last_1m_previous_year_data, left_on='full_address', right_on='page', how='left'))

        # Generic approach to handle NaN values based on column data type
        for col in self._data.columns:
            if pd.api.types.is_numeric_dtype(self._data[col]):
                self._data[col].fillna(0, inplace=True)  # Replace NaN with 0 for numeric columns
            else:
                self._data[col].fillna("", inplace=True)  # Replace NaN with empty string for non-numeric columns
                
    def store_data(self):
        website = WebsiteManager.get_website_by_project(self._project)

        for _, row in self._data.iterrows():
            GscManager.push_gscpage(
                full_address=row["full_address"],
                status_code=row["status_code"],
                redirect_url=row["redirect_url"],
                website=website,

                impressions_last_1m=row.get('impressions_last_1m', 0),
                impressions_last_16m=row.get('impressions_last_16m', 0),
                impressions_last_1m_previous_year=row.get('impressions_last_1m_previous_year', 0),
                impressions_previous_1m=row.get('impressions_previous_1m', 0),

                clicks_last_1m=row.get('clicks_last_1m', 0),
                clicks_last_16m=row.get('clicks_last_16m', 0),
                clicks_last_1m_previous_year=row.get('clicks_last_1m_previous_year', 0),
                clicks_previous_1m=row.get('clicks_previous_1m', 0),

                ctr_last_1m=row.get('ctr_last_1m', 0),
                ctr_last_16m=row.get('ctr_last_16m', 0),
                ctr_last_1m_previous_year=row.get('ctr_last_1m_previous_year', 0),
                ctr_previous_1m=row.get('ctr_previous_1m', 0),

                position_last_1m=row.get('position_last_1m', 0),
                position_last_16m=row.get('position_last_16m', 0),
                position_last_1m_previous_year=row.get('position_last_1m_previous_year', 0),
                position_previous_1m=row.get('position_previous_1m', 0),
            )
        logger.info("Data successfully processed using GscManager.")