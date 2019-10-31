BEGIN {
  t = "";
}
{
  if (NR <= 0) {
    next
  } else {
    if ((NR % 4) == 3) {
      n = split($0,x,"/");
      t = x[n];
    } else {
      if ((NR % 4) == 0) {
        printf "%s;00000000-0000-0000-00%02d-%012x;%s\n", t, p, c, $2;
        c++
      }
    }
  }
}
