# Logs Prosessing

Logs Processing API is provided to count distinct queries and to find top popular queries whithin datetime interval. 

## API 

### `GET /1/queries/count/<date_prefix>`
```
GET /1/queries/count/2015
```

```json
{ "count": 573697 }

```

### `GET /1/queries/popular/<date_prefix>?size=<size>`
```
GET /1/queries/popular/2015?size=3
```

```json
{
      "queries": [
        { "query": "http%3A%2F%2Fwww.getsidekick.com%2Fblog%2Fbody-language-advice", "count": 6675 },
        { "query": "http%3A%2F%2Fwebboard.yenta4.com%2Ftopic%2F568045", "count": 4652 },
        { "query": "http%3A%2F%2Fwebboard.yenta4.com%2Ftopic%2F379035%3Fsort%3D1", "count": 3100 }
      ]
    }
```

## How to run

### Setup
```
virtualenv -p python3.7 env && . env/bin/activate
pip install -r requirements.txt
```

### Build index
```
python3 build_index.py --raw_logs <I:RAW_LOGS_FILE> --logs_index <O:LOGS_INDEX_FILE> --query_index <O:QUERY_INDEX_FILE>
```

### Run
```
python3 server.py --logs_index <I:LOGS_INDEX_FILE> --query_index <I:QUERY_INDEX_FILE>
```


# Algorithm
## Logs index structure

```
{
    <year>: {
        <month>: {
            <day>: {
                "frequent_logs": { 
                    <query_id>: {
                        "_count": int,
                        <hour>: {
                            "_count": int, 
                            <minute>: int
                        }
                    }
                },
                "unique_logs": {
                    <query_id>: {
                        "_count": int,
                        <hour>: {
                            "_count": int,
                            <minute>: int
                        }
                    }
                }
            }
        }
    }
}
```

All logs are sorted by day.

Distinct queries per day, hour and minute are found looping the day queries and using "_count" field.

Distinct queries per month and year are found by merging queries and updating their counts from different days. To reduce the number of queries for merging, logs per day are divided into frequent logs and unique logs. Unique logs are those, that are met only within one day. So, we can only merge frequent logs and just add unique ones in the end.

Top elements are found using heap. We found top k elements from all frequent logs, top k elements from all unique logs, and finally top k elements from merged tops. The time complexity of getting top k queries in a list is O(n * lg(k)). So in our case it is O(n * lg(k)) + O(m * lg(k)) + O(2k * lg(k)) or O(max(n, m, k)*lg(k)).


