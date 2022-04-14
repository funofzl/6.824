i=0
fail_count=0
while [ $i -lt 100 ]
do
	go test -run Backup2B -race
	if [$? -ne 0] 
	then
		((fail_count++))
	fi
done 

echo $fail_count

