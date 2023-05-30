from persistence import rPost
from retry import retry
from tiktoken import get_encoding
from urllib.parse import urlparse
from fitz import open as open_pdf
import requests

# Set to a global variable to avoid calling the function every time.
enc = get_encoding("cl100k_base")

# Constants
# The maximum number of tokens we will use to compute embeddings.
MAX_TOKENS = 512
DIMENSIONS = 1536  # The number of dimensions of the embeddings.


def compute_embeddings(url: str) -> list[float]:
    """
    Compute the embeddings of a URL from the text of the article.
    First, we get the text of the article.
    Then, we shrink the text to MAX_TOKENS tokens.
    Finally, we compute the embeddings of the text.

    The dimension for text-embedding-ada-002 is 1536.

    Args:
        url (str): The URL of the article.

    Returns:
        list[float]: The embeddings of the article.
    """
    text = get_text(url)
    text = get_text_truncated_tokenized(text, MAX_TOKENS)

    print(text[:30])

    return [0.0 for _ in range(DIMENSIONS)]


def get_text(url: str) -> str:
    """
    Sort the type of URL and call the appropriate function to extract the text.

    Args:
        url (str): The URL of the article.
    """
    # Because all websites are different, we need to use different functions to
    # extract the text.
    # To know which function to use, we parse the URL.

    parsed = urlparse(url)

    if (parsed.hostname == "www.youtube.com" or parsed.hostname == "youtube.com" or parsed.hostname == "youtu.be"):
        return get_text_YouTube(url)

    # We check if the URL is a PDF.
    # To do so, we send a HEAD request to the URL and check the Content-Type.
    # If the Content-Type is application/pdf, we use the function get_text_pdf.
    # I prefer this solution to checking the extension because the extension
    # can be wrong (e.g. a PDF served without an extension).

    # We send a HEAD request to the URL.
    response = requests.head(url)

    # We check if the Content-Type is application/pdf.
    if response.headers["Content-Type"] == "application/pdf":
        return get_text_pdf(url)

    # We check if the URL is an image.
    matching_types = ["image/jpeg", "image/png",
                      "image/gif", "image/webp", "image/tiff", "image/bmp"]
    if response.headers["Content-Type"] in matching_types:
        # We raise an exception because we can't compute embeddings from an image.
        raise Exception("URL {} is an image. We can't extract text from an image.".format(
            url))

    # Before checking if a URL is an Arxiv article, we check if it's a PDF.
    # get_text_arxiv will modify the URL to get the PDF URL.
    # We don't want to modify the URL if it's already a PDF URL so we check before.

    if (parsed.hostname == "www.arxiv.org" or parsed.hostname == "arxiv.org"):
        return get_text_Arxiv(url)

    # If the URL didn't match any of the previous cases, we hope it's an article.
    return get_text_Article(url)


def get_text_Article(url: str) -> str:
    print("Article has not been implemented yet.")
    return ""


def get_text_YouTube(url: str) -> str:
    print("YouTube has not been implemented yet.")
    return ""


def get_text_pdf(url: str) -> str:

    # We download the PDF.
    # We use the stream parameter to avoid loading the whole PDF in memory.
    # We use the timeout parameter to avoid waiting too long for the PDF.
    response = requests.get(url, stream=True, timeout=15)

    # We open the PDF.
    # We use the context manager to close the PDF automatically.
    with open_pdf(stream=response.content, filetype="pdf") as pdf:
        # We get the text of the PDF.
        text = ""
        for page in pdf:
            text += page.get_text()

        # We return the text.
        return text


def get_text_Arxiv(url: str) -> str:
    # We extract the ID of the article.
    # The ID is the last part of the path.
    # For example, if the URL is https://arxiv.org/abs/1702.01715, the ID is 1702.01715.

    parsed = urlparse(url)
    path = parsed.path
    id = path.split("/")[-1]

    # We get the PDF URL.
    url = "https://arxiv.org/pdf/{}.pdf".format(id)
    return get_text_pdf(url)


def get_text_truncated_tokenized(text: str, max_tokens: int) -> str:
    """
    Truncate a text to the desired number of tokens.
    It's to avoid excessive costs when computing embeddings.

    Args:
        text (str): The text to truncate.
        max_tokens (int): The maximum number of tokens in cl100k_base

    """
    # We tokenize the text.
    tokens = enc.encode(text)

    # We truncate the tokens.
    tokens = tokens[:max_tokens]

    # We decode the tokens.
    text = enc.decode(tokens)

    # As stated here: https://learn.microsoft.com/en-us/azure/cognitive-services/openai/reference#embeddings
    # It's best to replace newlines with spaces.
    text = text.replace("\n", " ")

    return text


compute_embeddings("https://www.youtube.com/watch?v=dQw4w9WgXcQ")
compute_embeddings("https://arxiv.org/abs/1702.01715")
compute_embeddings("https://arxiv.org/pdf/2305.18179.pdf")
compute_embeddings("https://bitcoin.org/bitcoin.pdf")
compute_embeddings("https://python-rq.org/docs/workers/")
