#!/bin/bash
#!/bin/bash
cd src/raft
j=$10
for i in {0..9}
do
{
    go test -v  -run TestFailAgree2B
} &

done

wait