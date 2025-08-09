"Sync books from *arr pipeline to kobo books folder in NC via WebDav"

import os

from dotenv import load_dotenv
from webdav3.client import Client

load_dotenv(".home.env")

PASSWORD = os.environ.get("NC_PASSWORD")

if __name__ == "__main__":
    print(PASSWORD)
    # WebDAV url
    url = "https://nextcloud.paynepride.com/remote.php/dav/files/home/"

    options = {
        "webdav_hostname": url,
        "webdav_login": "home",
        "webdav_password": PASSWORD,
    }
    client = Client(options)
    client.verify = True
    # client.session.proxies(...)  # To set proxy directly into the session (Optional)
    # client.session.auth(...)  # To set proxy auth directly into the session (Optional)
    # client.execute_request("mkdir", "/directory_name")
    # breakpoint()
