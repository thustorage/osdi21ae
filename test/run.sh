threads=(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18)
# threads=(2 4 8 12 16 18)

for t in ${threads[@]}
do
echo ================ ${t} ====================
./aep_raw ${t}
done
