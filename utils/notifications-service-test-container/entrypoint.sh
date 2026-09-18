#!/bin/sh

curl -X 'POST' \
  'http://notifications-service:3000/api/notify' \
  -H 'accept: text/plain; charset=utf-8' \
  -H 'Content-Type: application/json; charset=utf-8' \
  -d '{
      "notification_type": "OutOfRange",
      "researcher": "d.landau@uu.nl",
      "measurement_id": "1234",
      "experiment_id": "5678",
      "cipher_data": "D5qnEHeIrTYmLwYX.hSZNb3xxQ9MtGhRP7E52yv2seWo4tUxYe28ATJVHUi0J++SFyfq5LQc0sTmiS4ILiM0/YsPHgp5fQKuRuuHLSyLA1WR9YIRS6nYrokZ68u4OLC4j26JW/QpiGmAydGKPIvV2ImD8t1NOUrejbnp/cmbMDUKO1hbXGPfD7oTvvk6JQVBAxSPVB96jDv7C4sGTmuEDZPoIpojcTBFP2xA"
  }' > /output/response.txt