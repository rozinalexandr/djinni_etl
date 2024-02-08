import requests
from bs4 import BeautifulSoup
from langdetect import detect

import csv
from typing import List, Optional


BASE_URL = "https://djinni.co"
CSV_HEADERS = ["position_title", "category", "company", "full_text", "url", "language", "parsing_date"]


def _get_input_categories(INPUT_FILE_PATH: str) -> List[str]:
    categories_list = []
    with open(INPUT_FILE_PATH, encoding="utf8") as f:
        for line in f:
            categories_list.append("+".join(line.strip().split(" ")))
    return categories_list


def _get_soup(url: str) -> BeautifulSoup:
    html = requests.get(url)
    return BeautifulSoup(html.text, features="html.parser")


def _get_data_from_tag(soup: BeautifulSoup,
                       tag_name: str,
                       tag_class: Optional[str] = None):
    try:
        return soup.find(tag_name, class_=tag_class).text.strip()
    except:
        return None


def _create_output_file(current_date: str, OUTPUT_FILE_PATH: str) -> None:
    with open(f"{OUTPUT_FILE_PATH + '/' + current_date}_raw.csv", "w", encoding="UTF8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEADERS)


def _save_category_to_csv(current_date: str,
                          data: List[List[str]],
                          OUTPUT_FILE_PATH: str) -> None:
    with open(f"{OUTPUT_FILE_PATH + '/' + current_date}_raw.csv", "a", encoding="UTF8", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(data)


def extract_djinni(INPUT_FILE_PATH: str, RAW_DATA_FILE_PATH: str, current_date: str):
    _create_output_file(current_date, RAW_DATA_FILE_PATH)
    categories_list = _get_input_categories(INPUT_FILE_PATH)

    for category in categories_list:
        base_soup = _get_soup(f"{BASE_URL}/jobs/?primary_keyword={category}")

        number_of_vacancies = int(base_soup.find("header").find("h1").find("span", class_="text-muted").text)
        number_of_pages = number_of_vacancies // 15

        all_vacancies_urls_list = []
        for page in range(1, number_of_pages + 1):
            page_soup = _get_soup(f"{BASE_URL}/jobs/?primary_keyword={category}&page={page}")

            vacancies_tags_list = (page_soup.find("ul", class_="list-unstyled list-jobs mb-4")
                                   .find_all("a", class_="h3 job-list-item__link"))

            vacancies_urls_list = [BASE_URL + tag.get("href") for tag in vacancies_tags_list]
            all_vacancies_urls_list.extend(vacancies_urls_list)

        all_vacancies_data_list = []
        for vacancy_url in all_vacancies_urls_list:
            vacancy_soup = _get_soup(vacancy_url)

            position_title = _get_data_from_tag(vacancy_soup, "h1").split("\n")[0]
            company = _get_data_from_tag(vacancy_soup, "a", "job-details--title")
            full_text = _get_data_from_tag(vacancy_soup, "div", "mb-4")
            language = detect(full_text)

            all_vacancies_data_list.append([position_title, category, company, full_text, vacancy_url, language,
                                            current_date])

        _save_category_to_csv(current_date, all_vacancies_data_list, RAW_DATA_FILE_PATH)
