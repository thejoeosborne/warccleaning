from __future__ import annotations
import requests
from typing import TypedDict
from concurrent import futures
from bs4 import BeautifulSoup
from warcio.archiveiterator import ArchiveIterator

# Using this sample file for the purposes of this exercise
SAMPLE_WARC_FILE = 'https://data.commoncrawl.org/crawl-data/CC-MAIN-2023-23/segments/1685224643388.45/warc/CC-MAIN-20230527223515-20230528013515-00003.warc.gz'


# Defining the structure of the returned data, just a simple list of dictionaries
class CleanedRecord(TypedDict):
    uri: str
    cleaned_text: str

def main(warc_file: str = SAMPLE_WARC_FILE) -> list[CleanedRecord]:

    # Get the warc file response
    # Note: This poses a situation with large files, as the entire file needs to download before processing,
    # which can take quite some time. Using the `stream=True` arg here helps begin the process quicker but messes
    # with the threading further down the line since not all data is available before processing it.
    # In a production environment I would most likely set up a pipeline to download the files beforehand to
    # have local copies to work with. I included a test function in this file to simply for loop through
    # a given number of records with `stream=True` set to you can quickly get a sample of the cleaning_function.

    res = requests.get(warc_file)

    # Set up the iterable
    archive_iterable = ArchiveIterator(res.raw)

    # Parallelize the cleaning of the text using threading
    cleaned_records = thread_items(archive_iterable, clean_text)

    return cleaned_records

def thread_items(items, func_to_execute):

    # Utility function to thread an iterable and return a list of the results

    future_results = []
    with futures.ThreadPoolExecutor() as executor:
        for func_arg in items:
            future_results.append(executor.submit(func_to_execute, func_arg))
    results = []
    for result in future_results:
        try:
            result_value = result.result()
            results.append(result_value)
        except Exception as err:
            print('issue with thread items')
            print(str(err))

    return results

def clean_text(record) -> CleanedRecord | None:

    # Grab the content and create a BeautifulSoup object
    if record.rec_type == 'response':
        content = record.content_stream().read()
        soup = BeautifulSoup(content, 'html.parser')
        uri = record.rec_headers.get_header('WARC-Target-URI')
    else:
        return

    # First remove all script, style, header, footer, and nav elements
    for element in soup(["script", "style", "header", "footer", "nav"]):
        element.extract()

    # Get the src of media tags and replace them with the tag and src
    media_tags = soup.find_all(['img', 'video', 'audio'])
    for media in media_tags:
        media_type = 'IMAGE' if media.name == 'img' else 'VIDEO' if media.name == 'video' else 'AUDIO'
        media_link = media.get('src', '')

        # Insert the media tag in the correct format. Prepend the base url if the src is relative
        inserted_html = f"""
        <{media_type}>{uri + media_link if 'http' not in media_link else media_link}</{media_type}>
        """
        media.replace_with(inserted_html)

    # Remove all html tags by getting the text
    raw_text = soup.get_text()

    # Remove unnecessary whitespace
    raw_text = " ".join(raw_text.split())

    return {'uri': uri, 'cleaned_text': raw_text}


def sample_cleaning_records(warc_file: str = SAMPLE_WARC_FILE, n: int = 500) -> list[CleanedRecord]:

    # Sample function to for loop through 100 records to test the cleaning function

    res = requests.get(warc_file, stream=True)

    # Set up the iterable
    archive_iterable = ArchiveIterator(res.raw)

    cleaned_records = []
    for i, record in enumerate(archive_iterable):
        print(f'Cleaning record {i}')
        cleaned_record = clean_text(record)
        if cleaned_record:
            cleaned_records.append(cleaned_record)
        if i == n:
            break

    return cleaned_records


if __name__ == '__main__':

    records = sample_cleaning_records()
