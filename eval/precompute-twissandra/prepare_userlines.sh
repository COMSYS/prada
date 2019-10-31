#!/bin/bash

rm -f users_maxoffsets.txt
for user in $(cat selected_users.txt | cut -f 1 -d";"); do
    notweets=$(grep -E " ${user}$" sorted_users.txt | sed "s/^ *//g" | cut -f 1 -d" ")
    printf "${user};${notweets}\n" >> users_maxoffsets.txt
done
