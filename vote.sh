#! /bin/bash

content=$(curl http://$RPC_IP:$PORT -X POST -H "Content-Type: application/json" -d '
  {
    "id":1,
    "jsonrpc":"2.0",
    "method":"getLatestBlockhash",
    "params":[
      {
        "commitment":"confirmed"
      }
    ]
  }
  ')
  echo "done1"
  blockhash=$(jq -r '.result.context.slot' <<<"$content")


  echo $blockhash
  validator_set=[\"tiv13UpNPRdZgazPCjRBxtff5SPuctQC1UmhHdGgUrc\",\"CV3F19YAhoW7DpfHQ5W9t2Zomb9h21NRi8k6hCA36Sk6\",\"ELY6u8Reinx7k2s9wgQA6jHHTsog7eN6qAYeTh2bNYx\"]
# echo $validator_set
#   #echo $body
  curl http://$RPC_IP:$PORT -X POST -H "Content-Type: application/json" -d @<( cat <<EOF
  {
    "id":1,
    "jsonrpc":"2.0",
    "method":"getVoteSignatures",
    "params":[
 $blockhash,
    { 
      "votePubkey": $validator_set,
      "commitment":"confirmed"
    } 
  ]
  }
EOF
) | jq
