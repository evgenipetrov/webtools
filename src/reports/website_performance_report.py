import pandas as pd

from exports.googlesearchconsole_page_query_last_1m_export import GoogleSearchConsolePageQueryExportLast1m
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

        self.file_name = REPORT_FILENAME

    def fetch_data(self):
        self.semrush_analytics_organic_positions_rootdomain_export.run()
        self.googlesearchconsole_page_query_last_1m_export.run()


    def process_data(self):
        screamingfrog_list_crawl_data = self.screamingfrog_list_crawl_export.get_data()
        semrush_analytics_organic_positions_rootdomain_data = self.semrush_analytics_organic_positions_rootdomain_export.get_data()
        googlesearchconsole_page_query_last_1m_data = self.googlesearchconsole_page_query_last_1m_export.get_data()

        processed_data = self._get_processed_data(screamingfrog_list_crawl_data)
        processed_data = self._join_semrush_analytics_organic_positions_rootdomain_by_volume(processed_data, semrush_analytics_organic_positions_rootdomain_data)
        processed_data = self._join_semrush_analytics_organic_positions_rootdomain_by_position(processed_data, semrush_analytics_organic_positions_rootdomain_data)

        ### adding GSC highest impression keyword
        # Selecting relevant columns and sorting by 'impressions'
        selected_columns = ["page", "clicks", "impressions", "ctr", "position"]
        join_data = googlesearchconsole_page_query_last_1m_data[selected_columns]

        # Group by 'page', sort by 'impressions' and keep the row with the highest 'impressions'
        join_data = join_data.sort_values(by='impressions', ascending=False).groupby('page').first().reset_index()

        # Add suffix to all columns in join_data except 'URL'
        join_key = "page"
        join_data.columns = [col + " (by impressions)" if col != join_key else col for col in join_data.columns]

        # Merge on 'URL'
        processed_data = pd.merge(processed_data, join_data, left_on="Address", right_on=join_key, how="outer")

        # Drop the 'URL' column as it's redundant
        processed_data.drop(columns=['page'], inplace=True)


        self.processed_data = processed_data

    def _join_semrush_analytics_organic_positions_rootdomain_by_position(self, processed_data, semrush_analytics_organic_positions_rootdomain_data):
        ### adding Semrush highest ranking keyword
        # Selecting relevant columns and sorting by 'Position'
        selected_columns = ["URL", "Keyword", "Position", "Search Volume"]
        join_data = semrush_analytics_organic_positions_rootdomain_data[selected_columns]
        # Group by 'URL', sort by 'Search Volume' and keep the row with the highest 'Search Volume'
        join_data = join_data.sort_values(by='Position', ascending=True).groupby('URL').first().reset_index()
        # Add suffix to all columns in join_data except 'URL'
        join_key = "URL"
        join_data.columns = [col + " (by position)" if col != join_key else col for col in join_data.columns]
        # Merge on 'URL'
        processed_data = pd.merge(processed_data, join_data, left_on="Address", right_on=join_key, how="outer")
        # Drop the 'URL' column as it's redundant
        processed_data.drop(columns=['URL'], inplace=True)
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
        join_data.columns = [col + " (by volume)" if col != join_key else col for col in join_data.columns]
        # Merge on 'URL'
        processed_data = pd.merge(processed_data, join_data, left_on="Address", right_on=join_key, how="outer")
        # Drop the 'URL' column as it's redundant
        processed_data.drop(columns=['URL'], inplace=True)
        return processed_data

    def _get_processed_data(self, screamingfrog_list_crawl_data):
        selected_columns = ["Address", "Status Code", "Redirect URL"]
        processed_data = screamingfrog_list_crawl_data[selected_columns]
        return processed_data

    def generate_report(self):
        self.report = self.processed_data
        return self.report
