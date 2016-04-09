$(sed -n "$1p" scp_commands.txt | sed "s/%/$2/g")
