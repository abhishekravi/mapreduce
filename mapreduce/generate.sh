:  > ssh_commands.txt
:  > scp_commands.txt
while read p;
do
	address=`aws ec2 describe-instances --instance-id $p | grep 'PublicDnsName' | head -n 1 | cut -d ':' -f2 | sed 's/\"//g' | sed 's/ //g' | sed 's/,//g'`

	echo "ssh -i testingec2.pem ec2-user@$address" >> ssh_commands.txt

	echo "scp -i testingec2.pem % ec2-user@$address:~/" >> scp_commands.txt
done < instances.txt
