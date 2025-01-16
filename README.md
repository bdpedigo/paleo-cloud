# Extracting features and labels

To build (from folder above):
`docker buildx build --platform linux/amd64 -t paleo-cloud .`

To run:
`docker run --rm --platform linux/amd64 -v /Users/ben.pedigo/.cloudvolume/secrets:/root/.cloudvolume/secrets paleo-cloud uv run ./runners/extract_info_2024-12-11.py`

To tag:
`docker tag paleo-cloud bdpedigo/paleo-cloud:v0`

To push:
`docker push bdpedigo/paleo-cloud:v0`

Making a cluster:
`sh ./make_cluster.sh`

Configuring a cluster:
`kubectl apply -f kube-task.yml`

Monitor the cluster:
`kubectl get pods`

Watch the logs in real-time:
`kubectl logs -f <pod-name>`

Check for issues with the cluster:
`kubectl describe nodes`
