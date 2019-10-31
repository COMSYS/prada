function getloc() {
  r = rand();
  if (r < 0.1) {return "UE";} # us-east
  if (r < 0.2) {return "UC";} # us-central
  if (r < 0.3) {return "UW";} # us-west
  if (r < 0.4) {return "CE";} # canada-east
  if (r < 0.5) {return "US";} # us-southcentral
  if (r < 0.6) {return "EW";} # europe-west
  if (r < 0.7) {return "AE";} # east-asia
  if (r < 0.8) {return "AS";} # southeast-asia
  if (r < 0.9) {return "JE";} # japan-east
  return "EN"; # europe-north
}


{
  user = $1;
  if (annots) {
    if (as[user] == 0) {
      as[user] = getloc();
    }
    a=" WITH ANNOTATIONS 'country' = { '" as[user] "' }";
  } else {
    a="";
  }
  printf "INSERT INTO twissandra.tweets (tweet_id, username, body) VALUES ('%s', '%s', '%s')%s;\n", $2, user, $3, a;
}

