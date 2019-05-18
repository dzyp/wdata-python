# CSV File Uploader

A simple command line tool for uploading files to WData Prep. Currently only uploads CSV files.

## Requirements

This is a python script and has only been tested with 2.7.

It uses only std library components so no pip install is required.

## Usage

The app requires oauth client id and secrets. More information about obtaining these can be found [here](https://success.workiva.com/developers/guides/setup).

Next a table id is required. It can be found in the path portion of the url in your browser when you are viewing a table in the Prep UI.

You'll need access to the file you want to upload locally. The file should be in an [RFC 4180](https://tools.ietf.org/html/rfc4180)-compliant format. The filename will be used to name the uploaded file and should include the `.csv` extension.

Download the python file in this repo and run locally in a terminal. 

Usage:
```console
python wdata_import.py <table_id> <client_id> <client_secret> <file_path>
```

`file_path` goes last which is useful for tools like `xargs`.
