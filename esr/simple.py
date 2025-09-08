from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import json
import time
import pathlib

def extract_caiso_charts_with_titles(url, headless=True):
    """
    Extract CAISO chart data with proper title detection

    Parameters:
    url (str): CAISO daily energy storage report URL
    headless (bool): Run browser in headless mode

    Returns:
    dict: Extracted chart data with titles
    """

    # Setup Chrome driver
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")

    driver = webdriver.Chrome(options=chrome_options)

    try:
        print(f"Loading: {url}")
        driver.get(url)

        # Wait for charts to load
        wait = WebDriverWait(driver, 30)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".highcharts-container, svg, canvas")))
        time.sleep(5)  # Additional wait for JavaScript

        # Enhanced script that better finds titles
        extraction_script = """
        var result = {
            titles: [],
            charts: []
        };

        // First, extract all potential titles from the page
        var titleElements = document.querySelectorAll('h1, h2, h3, h4, h5, h6, .title, [class*="title"], [class*="heading"]');
        titleElements.forEach(function(el) {
            var text = el.textContent.trim();
            if (text && text.length > 3 && text.length < 100) {
                result.titles.push({
                    text: text,
                    position: el.getBoundingClientRect(),
                    tagName: el.tagName,
                    className: el.className
                });
            }
        });

        // Extract Highcharts data
        if (typeof Highcharts !== 'undefined' && Highcharts.charts) {
            Highcharts.charts.forEach(function(chart, chartIndex) {
                if (chart && chart.series && chart.container) {
                    var chartRect = chart.container.getBoundingClientRect();

                    // Find the closest title above this chart
                    var bestTitle = 'Chart ' + chartIndex;
                    var bestDistance = Infinity;

                    result.titles.forEach(function(titleInfo) {
                        // Check if title is above the chart and reasonably close
                        if (titleInfo.position.bottom <= chartRect.top) {
                            var distance = chartRect.top - titleInfo.position.bottom;
                            var horizontalOverlap = Math.min(titleInfo.position.right, chartRect.right) -
                                                  Math.max(titleInfo.position.left, chartRect.left);

                            // Prefer titles that are close vertically and have some horizontal overlap
                            if (distance < 200 && horizontalOverlap > 0 && distance < bestDistance) {
                                bestDistance = distance;
                                bestTitle = titleInfo.text;
                            }
                        }
                    });

                    var chartData = {
                        chartIndex: chartIndex,
                        title: bestTitle,
                        chartPosition: chartRect,
                        series: []
                    };

                    chart.series.forEach(function(series) {
                        var seriesData = {
                            name: series.name || 'Series ' + series.index,
                            type: series.type,
                            data: series.data.map(function(point) {
                                var dataPoint = {
                                    x: point.x,
                                    y: point.y
                                };

                                // Handle datetime
                                if (typeof point.x === 'number' && point.x > 1000000000000) {
                                    dataPoint.datetime = new Date(point.x).toISOString();
                                }

                                // Handle categories
                                if (point.category !== undefined) {
                                    dataPoint.category = point.category;
                                }

                                return dataPoint;
                            })
                        };
                        chartData.series.push(seriesData);
                    });

                    result.charts.push(chartData);
                }
            });
        }

        return result;
        """

        # Execute the extraction script
        extracted_data = driver.execute_script(extraction_script)

        print(f"✓ Found {len(extracted_data['charts'])} charts")
        print(f"✓ Found {len(extracted_data['titles'])} potential titles")

        # Print the matched titles
        for chart in extracted_data['charts']:
            print(f"  Chart {chart['chartIndex']}: '{chart['title']}'")
            print(f"    Series: {[s['name'] for s in chart['series']]}")

        return extracted_data

    finally:
        driver.quit()

def convert_to_dataframes(extracted_data):
    """
    Convert extracted chart data to pandas DataFrames

    Parameters:
    extracted_data (dict): Output from extract_caiso_charts_with_titles

    Returns:
    dict: DataFrames organized by chart title
    """
    dataframes = {}

    for chart in extracted_data['charts']:
        chart_title = chart['title']

        # Combine all series into a single DataFrame for this chart
        all_data = []

        for series in chart['series']:
            for point in series['data']:
                row = {
                    'series_name': series['name'],
                    'value': point['y']
                }

                # Add time information
                if 'datetime' in point:
                    row['datetime'] = pd.to_datetime(point['datetime'])
                else:
                    row['x'] = point['x']

                # Add category if available
                if 'category' in point:
                    row['category'] = point['category']

                all_data.append(row)

        if all_data:
            df = pd.DataFrame(all_data)

            # If we have datetime, set it as index
            if 'datetime' in df.columns:
                df = df.set_index('datetime').sort_index()

            dataframes[chart_title] = df
            print(f"✓ Created DataFrame for '{chart_title}': {df.shape}")

    return dataframes



ANOMALOUS_DATES = {
    "may-08-2025": "may-8-2025",
    "jul-09-2025": "jul-9-2025",
}

def format_2022_url(date) -> str:
    date_str = date.strftime("%b%d-%Y").lower()
    date_str = ANOMALOUS_DATES.get(date_str, date_str)
    return (
        "https://www.caiso.com/documents/"
        f"dailyenergystoragereport{date_str}.html"
    )

def format_2023_url(date) -> str:
    date_str = date.strftime("%b%d-%Y").lower()
    date_str = ANOMALOUS_DATES.get(date_str, date_str)
    return (
        "https://www.caiso.com/documents/"
        f"dailyenergystoragereport{date_str}.html"
    )

def format_2024_url(date) -> str:
    if (date.month < 5) | ((date.month == 5) & (date.day < 24)):
        date_str = date.strftime("%b%d-%Y").lower()
        date_str = ANOMALOUS_DATES.get(date_str, date_str)
        return (
            "https://www.caiso.com/documents/"
            f"dailyenergystoragereport{date_str}.html"
        )
    else:
        if (date.month == 5) & (date.day >= 30):
            date_str = date.strftime("%b-%d%Y").lower()
            date_str = ANOMALOUS_DATES.get(date_str, date_str)
            return (
                "https://www.caiso.com/documents/"
                f"daily-energy-storage-report-{date_str}.html"
            )
        return format_2025_url(date)

def format_2025_url(date) -> str:
    date_str = date.strftime("%b-%d-%Y").lower()
    date_str = ANOMALOUS_DATES.get(date_str, date_str)
    return (
        "https://www.caiso.com/documents/"
        f"daily-energy-storage-report-{date_str}.html"
    )

def read_single_day_data(date) -> None:
    date = pd.to_datetime(date)

    if date.year == 2025:
        url = format_2025_url(date)
    elif date.year == 2024:
        url = format_2024_url(date)
    elif date.year == 2023:
        url = format_2023_url(date)
    elif date.year == 2022:
        url = format_2023_url(date)
    else:
        raise NotImplementedError("year =", date.year)

    data = extract_caiso_charts_with_titles(url, headless=True)
    dfs_and_names = convert_to_dataframes(data)

    #
    # will need to re-do this for hybrid
    #
    TARGET_DIR = pathlib.Path.cwd() / "esr_data" / "storage"
    TARGET_DIR.mkdir(parents=True, exist_ok=True)

    for chart_title, df in dfs_and_names.items():
        file_name = TARGET_DIR / (date.strftime("%Y%m%d") + "_" + chart_title + ".csv")
        df.to_csv(file_name)

    return None

if __name__ == "__main__":
    dates = pd.date_range("2022-07-31", "2022-12-31", freq="d")
    for date in dates:
        read_single_day_data(date)
