# beam-sandbox

Run pubsub emulator

```shell
gcloud beta emulators pubsub start --project=local-project

```

Run pubsub-load-gen

```shell
cd scripts/pubsub-load-gen

go get ./...

$(gcloud beta emulators pubsub env-init)

go run ./main.go -project=local-project -payload='{"ad_id":123,"source":"browse"}'
```

Run pipeline

```shell
python3.10 -m venv .venv

source .venv/bin/activate

pip install -r requirements.txt

$(gcloud beta emulators pubsub env-init)

python ./streaming_aggregator.py --project="local-project" --grouping_keys ad_id source 
```
