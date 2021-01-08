# metadata

This is the metadata service for CAVA.

Note: Ensure you are at repo root.

1. Build docker image

    ```bash
    export CI_REGISTRY_IMAGE=cormorack
    export TAG=test

    docker-compose -p io2_portal -f deployment/development/docker-compose.yml build metadata-service
    ```

2. Run the metadata service after build

    ```bash
    docker-compose -p io2_portal -f deployment/development/docker-compose.yml up metadata-service
    ```
