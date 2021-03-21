for file in *.txt
do
  echo "$file" 
  time -p ./par_sumsq ./"$file" "$1"
done
