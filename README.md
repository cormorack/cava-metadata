# cava-metadata

[![Cabled Array Official](https://tinyurl.com/ca-official)](#)

This is the metadata service for CAVA.

## Running the service

Note: Ensure you are at repo root.

1. Build docker image

    ```bash
    docker build -t cava-metadata:test -f ./resources/docker/Dockerfile .
    ```

2. Run the metadata service after build

    ```bash
    docker run --rm -it -p 80:80 cava-metadata:test
    ```

    - Interact with API for development: http://localhost/metadata/
  
   - To launch application for local development image use
   ```bash
   gunicorn -w 4 -k uvicorn.workers.UvicornWorker -b 127.0.0.1:8000 main:app
   ```

## Contribute

- Issue Tracker: https://github.com/cormorack/cava-metadata/issues
- Source Code: https://github.com/cormorack/cava-metadata

## Support

If you are having issues, please let us know.

## License

The project is licensed under the MIT license.
