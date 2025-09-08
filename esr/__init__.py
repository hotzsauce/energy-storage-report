from __future__ import annotations

import logging
import pandas as pd
import polars as pl
import requests

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException

import json
import time
import re
from datetime import datetime
from pathlib import Path



class CAISOStorageReportScraper:
    """
    Selenium-based scraper for CAISO Daily Energy Storage Reports
    """

    def __init__(self, headless=True, download_dir=None):
        """
        Initialize the scraper

        Parameters:
        headless (bool): Run browser in headless mode
        download_dir (str): Directory for downloads (if needed)
        """
        self.headless = headless
        self.download_dir = download_dir or "./caiso_downloads"
        Path(self.download_dir).mkdir(exist_ok=True)
        self.driver = None

    def setup_driver(self):
        """
        Set up Chrome WebDriver with appropriate options
        """
        chrome_options = Options()

        if self.headless:
            chrome_options.add_argument("--headless")

        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")

        # Set up download preferences
        prefs = {
            "download.default_directory": str(Path(self.download_dir).absolute()),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        chrome_options.add_experimental_option("prefs", prefs)

        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.implicitly_wait(10)
            print("✓ Chrome WebDriver initialized successfully")
        except Exception as e:
            print(f"✗ Error initializing WebDriver: {e}")
            print("Make sure ChromeDriver is installed and in your PATH")
            raise

    def load_report_page(self, url, wait_time=30):
        """
        Load a CAISO daily energy storage report page

        Parameters:
        url (str): URL of the report page
        wait_time (int): Maximum time to wait for page load
        """
        if not self.driver:
            self.setup_driver()

        print(f"Loading page: {url}")
        self.driver.get(url)

        # Wait for the page to load completely
        try:
            # Wait for charts or data containers to appear
            wait = WebDriverWait(self.driver, wait_time)

            # Look for common chart/data container elements
            possible_selectors = [
                "div[class*='chart']",
                "svg",
                "canvas",
                "div[id*='chart']",
                "div[class*='highcharts']",
                "div[class*='plotly']",
                "div[class*='d3']",
                ".chart-container",
                "#chart-container"
            ]

            for selector in possible_selectors:
                try:
                    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                    print(f"✓ Found chart element: {selector}")
                    break
                except TimeoutException:
                    continue
            else:
                print("⚠ No chart elements found, but page loaded")

            # Additional wait for JavaScript to execute
            time.sleep(5)

        except TimeoutException:
            print(f"⚠ Page load timeout after {wait_time} seconds")

    def extract_chart_data(self):
        """
        Extract data from charts on the page using various methods
        """
        data_sources = {}

        # Method 1: Look for data in JavaScript variables
        js_data = self.extract_js_data()
        if js_data:
            data_sources['javascript'] = js_data

        # Method 2: Extract data from chart libraries
        chart_data = self.extract_chart_library_data()
        if chart_data:
            data_sources['charts'] = chart_data

        # Method 3: Look for hidden data tables
        table_data = self.extract_table_data()
        if table_data:
            data_sources['tables'] = table_data

        # Method 4: Check for CSV/JSON download links
        download_links = self.find_download_links()
        if download_links:
            data_sources['downloads'] = download_links

        return data_sources

    def extract_js_data(self):
        """
        Extract data from JavaScript variables in the page
        """
        print("Searching for data in JavaScript variables...")

        # Common variable names that might contain chart data
        js_patterns = [
            "window.chartData",
            "window.data",
            "chartData",
            "plotData",
            "seriesData",
            "dataPoints",
            "energyData",
            "storageData"
        ]

        extracted_data = {}

        for pattern in js_patterns:
            try:
                # Try to extract the variable
                script = f"return typeof {pattern} !== 'undefined' ? {pattern} : null;"
                result = self.driver.execute_script(script)

                if result:
                    extracted_data[pattern] = result
                    print(f"✓ Found data in {pattern}")

            except Exception as e:
                print(f"  - Error extracting {pattern}: {e}")

        # Also search in script tags for data
        try:
            script_tags = self.driver.find_elements(By.TAG_NAME, "script")
            for i, script in enumerate(script_tags):
                script_content = script.get_attribute("innerHTML")
                if script_content and any(keyword in script_content.lower()
                                        for keyword in ['data', 'chart', 'series']):
                    # Try to extract JSON-like data
                    json_matches = re.findall(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', script_content)
                    for j, match in enumerate(json_matches[:3]):  # Limit to first 3 matches
                        try:
                            parsed = json.loads(match)
                            if isinstance(parsed, dict) and len(parsed) > 0:
                                extracted_data[f'script_{i}_json_{j}'] = parsed
                                print(f"✓ Found JSON data in script tag {i}")
                        except:
                            continue
        except Exception as e:
            print(f"  - Error searching script tags: {e}")

        return extracted_data

    def extract_chart_library_data(self):
        """
        Extract data from common chart libraries (Highcharts, Plotly, D3, etc.)
        """
        print("Searching for chart library data...")

        chart_data = {}

        # Highcharts
        try:
            highcharts_script = """
            var charts = [];
            if (typeof Highcharts !== 'undefined') {
                Highcharts.charts.forEach(function(chart, index) {
                    if (chart && chart.series) {
                        var chartData = {
                            title: chart.title ? chart.title.textStr : 'Chart ' + index,
                            series: []
                        };
                        chart.series.forEach(function(series) {
                            chartData.series.push({
                                name: series.name,
                                data: series.data.map(function(point) {
                                    return {x: point.x, y: point.y, category: point.category};
                                })
                            });
                        });
                        charts.push(chartData);
                    }
                });
            }
            return charts;
            """
            result = self.driver.execute_script(highcharts_script)
            if result:
                chart_data['highcharts'] = result
                print(f"✓ Found {len(result)} Highcharts")
        except Exception as e:
            print(f"  - Error extracting Highcharts: {e}")

        # Plotly
        try:
            plotly_script = """
            var plotlyData = [];
            if (typeof Plotly !== 'undefined') {
                var plots = document.querySelectorAll('.plotly-graph-div');
                plots.forEach(function(plot, index) {
                    if (plot && plot.data) {
                        plotlyData.push({
                            title: 'Plot ' + index,
                            data: plot.data
                        });
                    }
                });
            }
            return plotlyData;
            """
            result = self.driver.execute_script(plotly_script)
            if result:
                chart_data['plotly'] = result
                print(f"✓ Found {len(result)} Plotly charts")
        except Exception as e:
            print(f"  - Error extracting Plotly: {e}")

        return chart_data

    def extract_table_data(self):
        """
        Extract data from any tables on the page (including hidden ones)
        """
        print("Searching for table data...")

        table_data = []

        try:
            tables = self.driver.find_elements(By.TAG_NAME, "table")
            for i, table in enumerate(tables):
                try:
                    # Convert table to pandas DataFrame
                    html = table.get_attribute("outerHTML")
                    df = pd.read_html(html)[0]

                    if not df.empty:
                        table_data.append({
                            'table_index': i,
                            'data': df.to_dict('records'),
                            'columns': df.columns.tolist(),
                            'shape': df.shape
                        })
                        print(f"✓ Found table {i} with shape {df.shape}")

                except Exception as e:
                    print(f"  - Error processing table {i}: {e}")

        except Exception as e:
            print(f"  - Error finding tables: {e}")

        return table_data

    def find_download_links(self):
        """
        Find CSV, JSON, or Excel download links
        """
        print("Searching for download links...")

        download_links = []

        try:
            # Look for links with download-related text or file extensions
            links = self.driver.find_elements(By.TAG_NAME, "a")

            for link in links:
                href = link.get_attribute("href")
                text = link.text.lower()

                if href and any(ext in href.lower() for ext in ['.csv', '.json', '.xlsx', '.xls']):
                    download_links.append({
                        'url': href,
                        'text': link.text,
                        'type': 'file_link'
                    })
                    print(f"✓ Found download link: {link.text} ({href})")

                elif any(keyword in text for keyword in ['download', 'export', 'csv', 'excel']):
                    download_links.append({
                        'element': link,
                        'text': link.text,
                        'href': href,
                        'type': 'download_button'
                    })
                    print(f"✓ Found download button: {link.text}")

        except Exception as e:
            print(f"  - Error finding download links: {e}")

        return download_links

    def click_download_buttons(self, download_links):
        """
        Click on download buttons to trigger downloads
        """
        downloaded_files = []

        for link_info in download_links:
            if link_info['type'] == 'download_button':
                try:
                    element = link_info['element']
                    print(f"Clicking download button: {link_info['text']}")

                    # Scroll to element and click
                    self.driver.execute_script("arguments[0].scrollIntoView();", element)
                    time.sleep(1)
                    element.click()
                    time.sleep(3)  # Wait for download to start

                    downloaded_files.append(link_info)

                except Exception as e:
                    print(f"  - Error clicking {link_info['text']}: {e}")

        return downloaded_files

    def save_data(self, data_sources, output_file="extracted_data.json"):
        """
        Save extracted data to file
        """
        output_path = Path(self.download_dir) / output_file

        # Convert any non-serializable objects
        serializable_data = {}
        for source, data in data_sources.items():
            try:
                # Test if it's JSON serializable
                json.dumps(data)
                serializable_data[source] = data
            except (TypeError, ValueError):
                # Convert to string representation if not serializable
                serializable_data[source] = str(data)

        with open(output_path, 'w') as f:
            json.dump(serializable_data, f, indent=2, default=str)

        print(f"✓ Data saved to: {output_path}")
        return output_path

    def close(self):
        """
        Close the browser
        """
        if self.driver:
            self.driver.quit()
            print("✓ Browser closed")

def scrape_caiso_storage_report(url, output_file, headless=True):
    """
    Main function to scrape a CAISO energy storage report

    Parameters:
    url (str): URL of the CAISO daily energy storage report
    headless (bool): Run browser in headless mode
    """
    scraper = CAISOStorageReportScraper(headless=headless)

    try:
        # Load the page
        scraper.load_report_page(url)

        # Extract all available data
        data_sources = scraper.extract_chart_data()

        # Try to download any files
        if 'downloads' in data_sources:
            scraper.click_download_buttons(data_sources['downloads'])

        # Save the extracted data
        output_file = scraper.save_data(data_sources, output_file)

        # Print summary
        print("\n=== Extraction Summary ===")
        for source, data in data_sources.items():
            if isinstance(data, list):
                print(f"{source}: {len(data)} items")
            elif isinstance(data, dict):
                print(f"{source}: {len(data)} keys")
            else:
                print(f"{source}: {type(data)}")

        return data_sources, output_file

    finally:
        scraper.close()


def make_url(date: str) -> str:
    date_str = pd.to_datetime(date).strftime("%b-%d-%Y")
    if date_str.lower() == "may-08-2025":
        date_str = "may-8-2025"
    return (
        "https://www.caiso.com/documents/"
        f"daily-energy-storage-report-{date_str}.html"
    )


# Example usage
if __name__ == "__main__":

    dates = pd.date_range("2025-05-08", "2025-08-31", freq="d")
    for date in dates:
        url = make_url(date)
        output_file = f"esr_{date.strftime("%Y%m%d")}.json"

        data, output_file = scrape_caiso_storage_report(url, output_file, headless=True)

    print(dates)
    raise NotImplementedError("fdas")
    # URL from your original message
    url = "https://www.caiso.com/documents/daily-energy-storage-report-jan-31-2025.html"

    print("CAISO Energy Storage Report Scraper")
    print("=" * 50)

    # Run the scraper
    data, output_file = scrape_caiso_storage_report(url, headless=True)

    print(f"\nExtracted data saved to: {output_file}")
    print("Check the file for the complete data structure.")
