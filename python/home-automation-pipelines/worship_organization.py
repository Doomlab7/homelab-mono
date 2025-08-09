import os
from pathlib import Path

from dotenv import load_dotenv
from eyed3.mp3 import Mp3AudioFile
from webdav3.client import Client

load_dotenv(".home.env")

PASSWORD = os.environ.get("NC_PASSWORD")
TARGET = os.environ.get("NC_TARGET")
URL = os.environ.get("NC_URL")
USER = os.environ.get("NC_USER")

if __name__ == "__main__":
    options = {
        "webdav_hostname": URL,
        "webdav_login": USER,
        "webdav_password": PASSWORD,
    }
    client = Client(options)
    client.verify = True
    target = "/Olivet Bible/Worship/Music"
    lines = []
    for file in client.list(f"{target}"):
        if Path(file).suffix != ".mp3":
            continue
        save = "file.mp3"
        client.download_file(remote_path=f"/{target}/{file}", local_path=save)
        mp3 = Mp3AudioFile(save).initTag()
        lines.append((file, mp3.artist))
        # print(file)
        # print(mp3.artist)
    # save a csv
    import csv

    with open("music.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerows(lines)
        f.close()

    # for file in Path("/home/nic/personal/vocal-remover/downloads/mp3s/to-upload/").glob("*.mp3"):
    #     client.upload_file(remote_path=f"/{TARGET}/{file.name}", local_path=str(file))
    #     # move file from to-upload to uploaded
    #     file.rename(Path("/home/nic/personal/vocal-remover/downloads/mp3s/uploaded") / file.name)
