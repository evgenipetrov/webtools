import pandas as pd

from core.managers.url_manager import UrlManager
from exports.googleanalytics_last_1m_export import GoogleAnalytics4ExportLast1m
from exports.googleanalytics_previous_1m_export import GoogleAnalytics4ExportPrevious1m
from exports.googlesearchconsole_page_query_last_1m_export import GoogleSearchConsolePageQueryExportLast1m
from exports.googlesearchconsole_page_query_previous_1m_export import GoogleSearchConsolePageQueryExportPrevious1m
from exports.screamingfrog_list_crawl_export import ScreamingFrogListCrawlExport
from exports.semrush_analytics_organic_positions_rootdomain import SemrushAnalyticsOrganicPositionsRootdomainExport
from reports.base_report import BaseReport


class WebsitePerformanceReportRow:
    pass


REPORT_FILENAME = "website_performance_report.csv"


class WebsitePerformanceReport(BaseReport):
    def __init__(self, project):
        super().__init__(project)
        self.report = None
        self.processed_data = None

        self.screamingfrog_list_crawl_export = ScreamingFrogListCrawlExport(self.project)
        self.semrush_analytics_organic_positions_rootdomain_export = SemrushAnalyticsOrganicPositionsRootdomainExport(self.project)
        self.googlesearchconsole_page_query_last_1m_export = GoogleSearchConsolePageQueryExportLast1m(self.project)
        self.googlesearchconsole_page_query_previous_1m_export = GoogleSearchConsolePageQueryExportPrevious1m(self.project)
        self.googleanalytics4_last_1m_export = GoogleAnalytics4ExportLast1m(self.project)
        self.googleanalytics4_previous_1m_export = GoogleAnalytics4ExportPrevious1m(self.project)

        self.file_name = REPORT_FILENAME

    def fetch_data(self):
        self.semrush_analytics_organic_positions_rootdomain_export.run()
        self.googlesearchconsole_page_query_last_1m_export.run()
        self.googlesearchconsole_page_query_previous_1m_export.run()
        self.googleanalytics4_last_1m_export.run()
        self.googleanalytics4_previous_1m_export.run()

    def process_data(self):
        screamingfrog_list_crawl_data = self.screamingfrog_list_crawl_export.get_data()
        semrush_analytics_organic_positions_rootdomain_data = self.semrush_analytics_organic_positions_rootdomain_export.get_data()
        googlesearchconsole_page_query_last_1m_data = self.googlesearchconsole_page_query_last_1m_export.get_data()
        googlesearchconsole_page_query_previous_1m_data = self.googlesearchconsole_page_query_previous_1m_export.get_data()
        googleanalytics4_last_1m_data = self.googleanalytics4_last_1m_export.get_data()
        googleanalytics4_previous_1m_data = self.googleanalytics4_previous_1m_export.get_data()

        processed_data = self._generate_processed_data(screamingfrog_list_crawl_data)

        processed_data = self._join_semrush_analytics_organic_positions_rootdomain_by_volume(processed_data, semrush_analytics_organic_positions_rootdomain_data)
        processed_data = self._join_semrush_analytics_organic_positions_rootdomain_by_position(processed_data, semrush_analytics_organic_positions_rootdomain_data)

        processed_data = self._join_gsc_page_query_last_1m_by_impressions(processed_data, googlesearchconsole_page_query_last_1m_data)
        processed_data = self._join_gsc_page_query_last_1m_by_clicks(processed_data, googlesearchconsole_page_query_last_1m_data)
        processed_data = self._join_gsc_page_query_last_1m_by_position(processed_data, googlesearchconsole_page_query_last_1m_data)

        processed_data = self._join_gsc_page_query_previous_1m_by_impressions(processed_data, googlesearchconsole_page_query_previous_1m_data)
        processed_data = self._join_gsc_page_query_previous_1m_by_clicks(processed_data, googlesearchconsole_page_query_previous_1m_data)
        processed_data = self._join_gsc_page_query_previous_1m_by_position(processed_data, googlesearchconsole_page_query_previous_1m_data)

        processed_data = self._join_ga4_last_1m(processed_data, googleanalytics4_last_1m_data)
        processed_data = self._join_ga4_previous_1m(processed_data, googleanalytics4_previous_1m_data)

        self.processed_data = processed_data

    def _join_ga4_previous_1m(self, processed_data, googleanalytics4_previous_1m_data):
        selected_columns = ["pagePath", "sessionDefaultChannelGrouping", "sessions", "activeUsers", "activeUsers", "engagedSessions", "totalRevenue", "totalRevenue", "conversions"]
        join_data = googleanalytics4_previous_1m_data[selected_columns]

        # Filter only Organic Search
        join_data = join_data[join_data['sessionDefaultChannelGrouping'] == 'Organic Search']

        join_key = "pagePath"
        join_data.columns = [col + " (GA4 previous 1m)" if col != join_key else col for col in join_data.columns]
        # Merge on 'URL'
        processed_data = pd.merge(processed_data, join_data, left_on="Path", right_on=join_key, how="outer")
        # Drop the 'URL' column as it's redundant
        processed_data.drop(columns=[join_key], inplace=True)
        return processed_data

    def _join_ga4_last_1m(self, processed_data, googleanalytics4_last_1m_data):
        selected_columns = ["pagePath", "sessionDefaultChannelGrouping", "sessions", "activeUsers", "activeUsers", "engagedSessions", "totalRevenue", "totalRevenue", "conversions"]
        join_data = googleanalytics4_last_1m_data[selected_columns]

        # Filter only Organic Search
        join_data = join_data[join_data['sessionDefaultChannelGrouping'] == 'Organic Search']

        join_key = "pagePath"
        join_data.columns = [col + " (GA4 last 1m)" if col != join_key else col for col in join_data.columns]
        # Merge on 'URL'
        processed_data = pd.merge(processed_data, join_data, left_on="Path", right_on=join_key, how="outer")
        # Drop the 'URL' column as it's redundant
        processed_data.drop(columns=[join_key], inplace=True)
        return processed_data

    def _join_gsc_page_query_previous_1m_by_position(self, processed_data, googlesearchconsole_page_query_last_1m_data):
        ### adding GSC highest position keyword
        # Selecting relevant columns and sorting by 'position'
        selected_columns = ["page", "clicks", "impressions", "ctr", "position"]
        join_data = googlesearchconsole_page_query_last_1m_data[selected_columns]
        # Group by 'page', sort by 'impressions' and keep the row with the highest 'impressions'
        join_data = join_data.sort_values(by='position', ascending=True).groupby('page').first().reset_index()
        # Add suffix to all columns in join_data except 'URL'
        join_key = "page"
        join_data.columns = [col + " (GSC previous 1m by position)" if col != join_key else col for col in join_data.columns]
        # Merge on 'URL'
        processed_data = pd.merge(processed_data, join_data, left_on="Address", right_on=join_key, how="outer")
        # Drop the 'URL' column as it's redundant
        processed_data.drop(columns=[join_key], inplace=True)
        return processed_data

    def _join_gsc_page_query_previous_1m_by_clicks(self, processed_data, googlesearchconsole_page_query_last_1m_data):
        ### adding GSC highest clicks keyword
        # Selecting relevant columns and sorting by 'impressions'
        selected_columns = ["page", "clicks", "impressions", "ctr", "position"]
        join_data = googlesearchconsole_page_query_last_1m_data[selected_columns]
        # Group by 'page', sort by 'impressions' and keep the row with the highest 'impressions'
        join_data = join_data.sort_values(by='clicks', ascending=False).groupby('page').first().reset_index()
        # Add suffix to all columns in join_data except 'URL'
        join_key = "page"
        join_data.columns = [col + " (GSC previous 1m by clicks)" if col != join_key else col for col in join_data.columns]
        # Merge on 'URL'
        processed_data = pd.merge(processed_data, join_data, left_on="Address", right_on=join_key, how="outer")
        # Drop the 'URL' column as it's redundant
        processed_data.drop(columns=[join_key], inplace=True)
        return processed_data

    def _join_gsc_page_query_previous_1m_by_impressions(self, processed_data, googlesearchconsole_page_query_last_1m_data):
        ### adding GSC highest impression keyword
        # Selecting relevant columns and sorting by 'impressions'
        selected_columns = ["page", "clicks", "impressions", "ctr", "position"]
        join_data = googlesearchconsole_page_query_last_1m_data[selected_columns]
        # Group by 'page', sort by 'impressions' and keep the row with the highest 'impressions'
        join_data = join_data.sort_values(by='impressions', ascending=False).groupby('page').first().reset_index()
        # Add suffix to all columns in join_data except 'URL'
        join_key = "page"
        join_data.columns = [col + " (GSC previous 1m by impressions)" if col != join_key else col for col in join_data.columns]
        # Merge on 'URL'
        processed_data = pd.merge(processed_data, join_data, left_on="Address", right_on=join_key, how="outer")
        # Drop the 'URL' column as it's redundant
        processed_data.drop(columns=[join_key], inplace=True)
        return processed_data

    def _join_gsc_page_query_last_1m_by_position(self, processed_data, googlesearchconsole_page_query_last_1m_data):
        ### adding GSC highest position keyword
        # Selecting relevant columns and sorting by 'position'
        selected_columns = ["page", "clicks", "impressions", "ctr", "position"]
        join_data = googlesearchconsole_page_query_last_1m_data[selected_columns]
        # Group by 'page', sort by 'impressions' and keep the row with the highest 'impressions'
        join_data = join_data.sort_values(by='position', ascending=True).groupby('page').first().reset_index()
        # Add suffix to all columns in join_data except 'URL'
        join_key = "page"
        join_data.columns = [col + " (GSC last 1m by position)" if col != join_key else col for col in join_data.columns]
        # Merge on 'URL'
        processed_data = pd.merge(processed_data, join_data, left_on="Address", right_on=join_key, how="outer")
        # Drop the 'URL' column as it's redundant
        processed_data.drop(columns=[join_key], inplace=True)
        return processed_data

    def _join_gsc_page_query_last_1m_by_clicks(self, processed_data, googlesearchconsole_page_query_last_1m_data):
        ### adding GSC highest clicks keyword
        # Selecting relevant columns and sorting by 'impressions'
        selected_columns = ["page", "clicks", "impressions", "ctr", "position"]
        join_data = googlesearchconsole_page_query_last_1m_data[selected_columns]
        # Group by 'page', sort by 'impressions' and keep the row with the highest 'impressions'
        join_data = join_data.sort_values(by='clicks', ascending=False).groupby('page').first().reset_index()
        # Add suffix to all columns in join_data except 'URL'
        join_key = "page"
        join_data.columns = [col + " (GSC last 1m by clicks)" if col != join_key else col for col in join_data.columns]
        # Merge on 'URL'
        processed_data = pd.merge(processed_data, join_data, left_on="Address", right_on=join_key, how="outer")
        # Drop the 'URL' column as it's redundant
        processed_data.drop(columns=[join_key], inplace=True)
        return processed_data

    def _join_gsc_page_query_last_1m_by_impressions(self, processed_data, googlesearchconsole_page_query_last_1m_data):
        ### adding GSC highest impression keyword
        # Selecting relevant columns and sorting by 'impressions'
        selected_columns = ["page", "clicks", "impressions", "ctr", "position"]
        join_data = googlesearchconsole_page_query_last_1m_data[selected_columns]
        # Group by 'page', sort by 'impressions' and keep the row with the highest 'impressions'
        join_data = join_data.sort_values(by='impressions', ascending=False).groupby('page').first().reset_index()
        # Add suffix to all columns in join_data except 'URL'
        join_key = "page"
        join_data.columns = [col + " (GSC last 1m by impressions)" if col != join_key else col for col in join_data.columns]
        # Merge on 'URL'
        processed_data = pd.merge(processed_data, join_data, left_on="Address", right_on=join_key, how="outer")
        # Drop the 'URL' column as it's redundant
        processed_data.drop(columns=[join_key], inplace=True)
        return processed_data

    def _join_semrush_analytics_organic_positions_rootdomain_by_position(self, processed_data, semrush_analytics_organic_positions_rootdomain_data):
        ### adding Semrush highest ranking keyword
        # Selecting relevant columns and sorting by 'Position'
        selected_columns = ["URL", "Keyword", "Position", "Search Volume"]
        join_data = semrush_analytics_organic_positions_rootdomain_data[selected_columns]
        # Group by 'URL', sort by 'Search Volume' and keep the row with the highest 'Search Volume'
        join_data = join_data.sort_values(by='Position', ascending=True).groupby('URL').first().reset_index()
        # Add suffix to all columns in join_data except 'URL'
        join_key = "URL"
        join_data.columns = [col + " (Semrush by position)" if col != join_key else col for col in join_data.columns]
        # Merge on 'URL'
        processed_data = pd.merge(processed_data, join_data, left_on="Address", right_on=join_key, how="outer")
        # Drop the 'URL' column as it's redundant
        processed_data.drop(columns=[join_key], inplace=True)
        return processed_data

    def _join_semrush_analytics_organic_positions_rootdomain_by_volume(self, processed_data, semrush_analytics_organic_positions_rootdomain_data):
        ### adding Semrush highest volume keyword
        # Selecting relevant columns and sorting by 'Search Volume'
        selected_columns = ["URL", "Keyword", "Position", "Search Volume"]
        join_data = semrush_analytics_organic_positions_rootdomain_data[selected_columns]
        # Group by 'URL', sort by 'Search Volume' and keep the row with the highest 'Search Volume'
        join_data = join_data.sort_values(by='Search Volume', ascending=False).groupby('URL').first().reset_index()
        # Add suffix to all columns in join_data except 'URL'
        join_key = "URL"
        join_data.columns = [col + " (Semrush by volume)" if col != join_key else col for col in join_data.columns]
        # Merge on 'URL'
        processed_data = pd.merge(processed_data, join_data, left_on="Address", right_on=join_key, how="outer")
        # Drop the 'URL' column as it's redundant
        processed_data.drop(columns=[join_key], inplace=True)
        return processed_data

    def _generate_processed_data(self, screamingfrog_list_crawl_data):
        selected_columns = ["Address", "Status Code", "Redirect URL"]
        processed_data = screamingfrog_list_crawl_data[selected_columns]
        # Assuming processed_data is a subset/slice of another DataFrame
        # Create an explicit copy to avoid the SettingWithCopyWarning
        processed_data = processed_data.copy()

        # Now apply the changes to the copy
        processed_data['Path'] = processed_data['Address'].apply(UrlManager.get_relative_url)

        return processed_data

    def generate_report(self):
        self.report = self.processed_data
        return self.report
