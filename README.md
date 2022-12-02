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

## Contribute

- Issue Tracker: https://github.com/cormorack/cava-metadata/issues
- Source Code: https://github.com/cormorack/cava-metadata

## Support

If you are having issues, please let us know.

## License

The project is licensed under the MIT license.
