# metadata

This is the metadata service for CAVA.

Note: Ensure you are at repo root.

1. Build docker image

    ```bash
    docker build -t cava-metadata:test -f ./resources/docker/Dockerfile .
    ```

2. Run the metadata service after build

    ```bash
    docker run --rm -it -p 80:80 cava-metadata:test
    ```
