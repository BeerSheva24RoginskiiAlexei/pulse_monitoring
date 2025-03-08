# Task definition

## Writing LatestValuesSaverMap class implementing LatestValuesSaver interface
### Simple implementation based on HashMap in the project last-values-saver-impl
## Update jump-pulse-recognizer
### Uses LatestValuesSaver 
### Introduces environment variable FACTOR with default value 0.5
### Recognizes whether there a jump by comparing a last value with the current one
Math.abs(last - current) / (float)last >= factor is criteria of a jump
### Uses MiddlewareDataStream
 put jump data inside new created DynamoDB table "jump-pulse-values" as data stream

