import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class BioPartitioner extends Partitioner<LongWritable,LongWritable> {
  @Override
  //public int getPartition(IntWritable DNA_prefix, LongWritable value, int numReduceTasks) {
  public int getPartition(LongWritable DNA_prefix, LongWritable value, int numReduceTasks) {

//32G 13 chars
//if(DNA_prefix.get() <= 311504366L)
//  return 31;
//
//if(DNA_prefix.get() > 311504366L && DNA_prefix.get() <= 327835406L)
//  return 30;
//
//if(DNA_prefix.get() > 327835406L && DNA_prefix.get() <= 341762500L)
//  return 29;
//
//if(DNA_prefix.get() > 341762500L && DNA_prefix.get() <= 364841408L)
//  return 28;
//
//if(DNA_prefix.get() > 364841408L && DNA_prefix.get() <= 402921068L)
//  return 27;
//
//if(DNA_prefix.get() > 402921068L && DNA_prefix.get() <= 419372869L)
//  return 26;
//
//if(DNA_prefix.get() > 419372869L && DNA_prefix.get() <= 439168083L)
//  return 25;
//
//if(DNA_prefix.get() > 439168083L && DNA_prefix.get() <= 467915609L)
//  return 24;
//
//if(DNA_prefix.get() > 467915609L && DNA_prefix.get() <= 484270947L)
//  return 23;
//
//if(DNA_prefix.get() > 484270947L && DNA_prefix.get() <= 555178125L)
//  return 22;
//
//if(DNA_prefix.get() > 555178125L && DNA_prefix.get() <= 571238617L)
//  return 21;
//
//if(DNA_prefix.get() > 571238617L && DNA_prefix.get() <= 585851846L)
//  return 20;
//
//if(DNA_prefix.get() > 585851846L && DNA_prefix.get() <= 628624349L)
//  return 19;
//
//if(DNA_prefix.get() > 628624349L && DNA_prefix.get() <= 696083712L)
//  return 18;
//
//if(DNA_prefix.get() > 696083712L && DNA_prefix.get() <= 717466466L)
//  return 17;
//
//if(DNA_prefix.get() > 717466466L && DNA_prefix.get() <= 732233311L)
//  return 16;
//
//if(DNA_prefix.get() > 732233311L && DNA_prefix.get() <= 812991875L)
//  return 15;
//
//if(DNA_prefix.get() > 812991875L && DNA_prefix.get() <= 843457733L)
//  return 14;
//
//if(DNA_prefix.get() > 843457733L && DNA_prefix.get() <= 874941682L)
//  return 13;
//
//if(DNA_prefix.get() > 874941682L && DNA_prefix.get() <= 910671209L)
//  return 12;
//
//if(DNA_prefix.get() > 910671209L && DNA_prefix.get() <= 946988448L)
//  return 11;
//
//if(DNA_prefix.get() > 946988448L && DNA_prefix.get() <= 966792407L)
//  return 10;
//
//if(DNA_prefix.get() > 966792407L && DNA_prefix.get() <= 1040222795L)
//  return 9;
//
//if(DNA_prefix.get() > 1040222795L && DNA_prefix.get() <= 1063477809L)
//  return 8;
//
//if(DNA_prefix.get() > 1063477809L && DNA_prefix.get() <= 1090417683L)
//  return 7;
//
//if(DNA_prefix.get() > 1090417683L && DNA_prefix.get() <= 1117774786L)
//  return 6;
//
//if(DNA_prefix.get() > 1117774786L && DNA_prefix.get() <= 1141113323L)
//  return 5;
//
//if(DNA_prefix.get() > 1141113323L && DNA_prefix.get() <= 1160021869L)
//  return 4;
//
//if(DNA_prefix.get() > 1160021869L && DNA_prefix.get() <= 1171868486L)
//  return 3;
//
//if(DNA_prefix.get() > 1171868486L && DNA_prefix.get() <= 1197151871L)
//  return 2;
//
//if(DNA_prefix.get() > 1197151871L && DNA_prefix.get() <= 1214181037L)
//  return 1;
//
//return 0;

//64G
//if(DNA_prefix.get() <= 307181224)
//  return 63;
//
//if(DNA_prefix.get() > 307181224 && DNA_prefix.get() <= 311426448)
//  return 62;
//
//if(DNA_prefix.get() > 311426448 && DNA_prefix.get() <= 317145161)
//  return 61;
//
//if(DNA_prefix.get() > 317145161 && DNA_prefix.get() <= 327499793)
//  return 60;
//
//if(DNA_prefix.get() > 327499793 && DNA_prefix.get() <= 335852057)
//  return 59;
//
//if(DNA_prefix.get() > 335852057 && DNA_prefix.get() <= 341709309)
//  return 58;
//
//if(DNA_prefix.get() > 341709309 && DNA_prefix.get() <= 358088707)
//  return 57;
//
//if(DNA_prefix.get() > 358088707 && DNA_prefix.get() <= 364840466)
//  return 56;
//
//if(DNA_prefix.get() > 364840466 && DNA_prefix.get() <= 384631032)
//  return 55;
//
//if(DNA_prefix.get() > 384631032 && DNA_prefix.get() <= 402885836)
//  return 54;
//
//if(DNA_prefix.get() > 402885836 && DNA_prefix.get() <= 409475867)
//  return 53;
//
//if(DNA_prefix.get() > 409475867 && DNA_prefix.get() <= 419505324)
//  return 52;
//
//if(DNA_prefix.get() > 419505324 && DNA_prefix.get() <= 432148708)
//  return 51;
//
//if(DNA_prefix.get() > 432148708 && DNA_prefix.get() <= 439218736)
//  return 50;
//
//if(DNA_prefix.get() > 439218736 && DNA_prefix.get() <= 457731161)
//  return 49;
//
//if(DNA_prefix.get() > 457731161 && DNA_prefix.get() <= 468069531)
//  return 48;
//
//if(DNA_prefix.get() > 468069531 && DNA_prefix.get() <= 476562500)
//  return 47;
//
//if(DNA_prefix.get() > 476562500 && DNA_prefix.get() <= 484288750)
//  return 46;
//
//if(DNA_prefix.get() > 484288750 && DNA_prefix.get() <= 549334061)
//  return 45;
//
//if(DNA_prefix.get() > 549334061 && DNA_prefix.get() <= 555224993)
//  return 44;
//
//if(DNA_prefix.get() > 555224993 && DNA_prefix.get() <= 563416707)
//  return 43;
//
//if(DNA_prefix.get() > 563416707 && DNA_prefix.get() <= 571714532)
//  return 42;
//
//if(DNA_prefix.get() > 571714532 && DNA_prefix.get() <= 579919614)
//  return 41;
//
//if(DNA_prefix.get() > 579919614 && DNA_prefix.get() <= 585888699)
//  return 40;
//
//if(DNA_prefix.get() > 585888699 && DNA_prefix.get() <= 605256243)
//  return 39;
//
//if(DNA_prefix.get() > 605256243 && DNA_prefix.get() <= 628107873)
//  return 38;
//
//if(DNA_prefix.get() > 628107873 && DNA_prefix.get() <= 653004349)
//  return 37;
//
//if(DNA_prefix.get() > 653004349 && DNA_prefix.get() <= 696232308)
//  return 36;
//
//if(DNA_prefix.get() > 696232308 && DNA_prefix.get() <= 707991698)
//  return 35;
//
//if(DNA_prefix.get() > 707991698 && DNA_prefix.get() <= 717498749)
//  return 34;
//
//if(DNA_prefix.get() > 717498749 && DNA_prefix.get() <= 725308439)
//  return 33;
//
//if(DNA_prefix.get() > 725308439 && DNA_prefix.get() <= 732389309)
//  return 32;
//
//if(DNA_prefix.get() > 732389309 && DNA_prefix.get() <= 799707656)
//  return 31;
//
//if(DNA_prefix.get() > 799707656 && DNA_prefix.get() <= 813029961)
//  return 30;
//
//if(DNA_prefix.get() > 813029961 && DNA_prefix.get() <= 823196618)
//  return 29;
//
//if(DNA_prefix.get() > 823196618 && DNA_prefix.get() <= 843543711)
//  return 28;
//
//if(DNA_prefix.get() > 843543711 && DNA_prefix.get() <= 854051573)
//  return 27;
//
//if(DNA_prefix.get() > 854051573 && DNA_prefix.get() <= 875520366)
//  return 26;
//
//if(DNA_prefix.get() > 875520366 && DNA_prefix.get() <= 895097971)
//  return 25;
//
//if(DNA_prefix.get() > 895097971 && DNA_prefix.get() <= 910748691)
//  return 24;
//
//if(DNA_prefix.get() > 910748691 && DNA_prefix.get() <= 925382656)
//  return 23;
//
//if(DNA_prefix.get() > 925382656 && DNA_prefix.get() <= 946871661)
//  return 22;
//
//if(DNA_prefix.get() > 946871661 && DNA_prefix.get() <= 960134168)
//  return 21;
//
//if(DNA_prefix.get() > 960134168 && DNA_prefix.get() <= 966796875)
//  return 20;
//
//if(DNA_prefix.get() > 966796875 && DNA_prefix.get() <= 976349688)
//  return 19;
//
//if(DNA_prefix.get() > 976349688 && DNA_prefix.get() <= 1040851496)
//  return 18;
//
//if(DNA_prefix.get() > 1040851496 && DNA_prefix.get() <= 1049724374)
//  return 17;
//
//if(DNA_prefix.get() > 1049724374 && DNA_prefix.get() <= 1064030997)
//  return 16;
//
//if(DNA_prefix.get() > 1064030997 && DNA_prefix.get() <= 1073413984)
//  return 15;
//
//if(DNA_prefix.get() > 1073413984 && DNA_prefix.get() <= 1090546564)
//  return 14;
//
//if(DNA_prefix.get() > 1090546564 && DNA_prefix.get() <= 1098416561)
//  return 13;
//
//if(DNA_prefix.get() > 1098416561 && DNA_prefix.get() <= 1118102699)
//  return 12;
//
//if(DNA_prefix.get() > 1118102699 && DNA_prefix.get() <= 1135352923)
//  return 11;
//
//if(DNA_prefix.get() > 1135352923 && DNA_prefix.get() <= 1141321682)
//  return 10;
//
//if(DNA_prefix.get() > 1141321682 && DNA_prefix.get() <= 1151128918)
//  return 9;
//
//if(DNA_prefix.get() > 1151128918 && DNA_prefix.get() <= 1160156250)
//  return 8;
//
//if(DNA_prefix.get() > 1160156250 && DNA_prefix.get() <= 1168552412)
//  return 7;
//
//if(DNA_prefix.get() > 1168552412 && DNA_prefix.get() <= 1171875000)
//  return 6;
//
//if(DNA_prefix.get() > 1171875000 && DNA_prefix.get() <= 1190043124)
//  return 5;
//
//if(DNA_prefix.get() > 1190043124 && DNA_prefix.get() <= 1197183664)
//  return 4;
//
//if(DNA_prefix.get() > 1197183664 && DNA_prefix.get() <= 1207519531)
//  return 3;
//
//if(DNA_prefix.get() > 1207519531 && DNA_prefix.get() <= 1214202172)
//  return 2;
//
//if(DNA_prefix.get() > 1214202172 && DNA_prefix.get() <= 1218476469)
//  return 1;
//
//return 0;


//137 G 20 chars
//if(DNA_prefix.get() <= 24013662066608L)
//  return 63;
//
//if(DNA_prefix.get() > 24013662066608L && DNA_prefix.get() <= 24377409328072L)
//  return 62;
//
//if(DNA_prefix.get() > 24377409328072L && DNA_prefix.get() <= 25070133355924L)
//  return 61;
//
//if(DNA_prefix.get() > 25070133355924L && DNA_prefix.get() <= 25784292145219L)
//  return 60;
//
//if(DNA_prefix.get() > 25784292145219L && DNA_prefix.get() <= 26443326168448L)
//  return 59;
//
//if(DNA_prefix.get() > 26443326168448L && DNA_prefix.get() <= 27690764191419L)
//  return 58;
//
//if(DNA_prefix.get() > 27690764191419L && DNA_prefix.get() <= 28063652211875L)
//  return 57;
//
//if(DNA_prefix.get() > 28063652211875L && DNA_prefix.get() <= 28909887701418L)
//  return 56;
//
//if(DNA_prefix.get() > 28909887701418L && DNA_prefix.get() <= 30252636327736L)
//  return 55;
//
//if(DNA_prefix.get() > 30252636327736L && DNA_prefix.get() <= 31636082802808L)
//  return 54;
//
//if(DNA_prefix.get() > 31636082802808L && DNA_prefix.get() <= 32325858929167L)
//  return 53;
//
//if(DNA_prefix.get() > 32325858929167L && DNA_prefix.get() <= 33180979272868L)
//  return 52;
//
//if(DNA_prefix.get() > 33180979272868L && DNA_prefix.get() <= 34065745679582L)
//  return 51;
//
//if(DNA_prefix.get() > 34065745679582L && DNA_prefix.get() <= 35438537881173L)
//  return 50;
//
//if(DNA_prefix.get() > 35438537881173L && DNA_prefix.get() <= 36132796764863L)
//  return 49;
//
//if(DNA_prefix.get() > 36132796764863L && DNA_prefix.get() <= 36987253637307L)
//  return 48;
//
//if(DNA_prefix.get() > 36987253637307L && DNA_prefix.get() <= 37648576832431L)
//  return 47;
//
//if(DNA_prefix.get() > 37648576832431L && DNA_prefix.get() <= 38101155350911L)
//  return 46;
//
//if(DNA_prefix.get() > 38101155350911L && DNA_prefix.get() <= 43090789635296L)
//  return 45;
//
//if(DNA_prefix.get() > 43090789635296L && DNA_prefix.get() <= 43739387713416L)
//  return 44;
//
//if(DNA_prefix.get() > 43739387713416L && DNA_prefix.get() <= 44464111328125L)
//  return 43;
//
//if(DNA_prefix.get() > 44464111328125L && DNA_prefix.get() <= 44949951171874L)
//  return 42;
//
//if(DNA_prefix.get() > 44949951171874L && DNA_prefix.get() <= 45621771429292L)
//  return 41;
//
//if(DNA_prefix.get() > 45621771429292L && DNA_prefix.get() <= 47048912617067L)
//  return 40;
//
//if(DNA_prefix.get() > 47048912617067L && DNA_prefix.get() <= 47760009239058L)
//  return 39;
//
//if(DNA_prefix.get() > 47760009239058L && DNA_prefix.get() <= 49241429497823L)
//  return 38;
//
//if(DNA_prefix.get() > 49241429497823L && DNA_prefix.get() <= 51372068280992L)
//  return 37;
//
//if(DNA_prefix.get() > 51372068280992L && DNA_prefix.get() <= 53293206506089L)
//  return 36;
//
//if(DNA_prefix.get() > 53293206506089L && DNA_prefix.get() <= 55189777849031L)
//  return 35;
//
//if(DNA_prefix.get() > 55189777849031L && DNA_prefix.get() <= 55952864412499L)
//  return 34;
//
//if(DNA_prefix.get() > 55952864412499L && DNA_prefix.get() <= 56426938005342L)
//  return 33;
//
//if(DNA_prefix.get() > 56426938005342L && DNA_prefix.get() <= 57189941406250L)
//  return 32;
//
//if(DNA_prefix.get() > 57189941406250L && DNA_prefix.get() <= 62406559635822L)
//  return 31;
//
//if(DNA_prefix.get() > 62406559635822L && DNA_prefix.get() <= 63521140543284L)
//  return 30;
//
//if(DNA_prefix.get() > 63521140543284L && DNA_prefix.get() <= 64341742936831L)
//  return 29;
//
//if(DNA_prefix.get() > 64341742936831L && DNA_prefix.get() <= 65957634259364L)
//  return 28;
//
//if(DNA_prefix.get() > 65957634259364L && DNA_prefix.get() <= 66648667804609L)
//  return 27;
//
//if(DNA_prefix.get() > 66648667804609L && DNA_prefix.get() <= 68123776902748L)
//  return 26;
//
//if(DNA_prefix.get() > 68123776902748L && DNA_prefix.get() <= 69650243351406L)
//  return 25;
//
//if(DNA_prefix.get() > 69650243351406L && DNA_prefix.get() <= 70556376537500L)
//  return 24;
//
//if(DNA_prefix.get() > 70556376537500L && DNA_prefix.get() <= 71533139712448L)
//  return 23;
//
//if(DNA_prefix.get() > 71533139712448L && DNA_prefix.get() <= 72479248046875L)
//  return 22;
//
//if(DNA_prefix.get() > 72479248046875L && DNA_prefix.get() <= 74299918933114L)
//  return 21;
//
//if(DNA_prefix.get() > 74299918933114L && DNA_prefix.get() <= 75223388671875L)
//  return 20;
//
//if(DNA_prefix.get() > 75223388671875L && DNA_prefix.get() <= 75913490879359L)
//  return 19;
//
//if(DNA_prefix.get() > 75913490879359L && DNA_prefix.get() <= 81066644371499L)
//  return 18;
//
//if(DNA_prefix.get() > 81066644371499L && DNA_prefix.get() <= 81581235073473L)
//  return 17;
//
//if(DNA_prefix.get() > 81581235073473L && DNA_prefix.get() <= 82394420574966L)
//  return 16;
//
//if(DNA_prefix.get() > 82394420574966L && DNA_prefix.get() <= 83465428882683L)
//  return 15;
//
//if(DNA_prefix.get() > 83465428882683L && DNA_prefix.get() <= 84899293937124L)
//  return 14;
//
//if(DNA_prefix.get() > 84899293937124L && DNA_prefix.get() <= 85430877457074L)
//  return 13;
//
//if(DNA_prefix.get() > 85430877457074L && DNA_prefix.get() <= 86512792835166L)
//  return 12;
//
//if(DNA_prefix.get() > 86512792835166L && DNA_prefix.get() <= 87552799762049L)
//  return 11;
//
//if(DNA_prefix.get() > 87552799762049L && DNA_prefix.get() <= 88926161476219L)
//  return 10;
//
//if(DNA_prefix.get() > 88926161476219L && DNA_prefix.get() <= 89562244915424L)
//  return 9;
//
//if(DNA_prefix.get() > 89562244915424L && DNA_prefix.get() <= 90372985489335L)
//  return 8;
//
//if(DNA_prefix.get() > 90372985489335L && DNA_prefix.get() <= 91084560156246L)
//  return 7;
//
//if(DNA_prefix.get() > 91084560156246L && DNA_prefix.get() <= 91489198177906L)
//  return 6;
//
//if(DNA_prefix.get() > 91489198177906L && DNA_prefix.get() <= 92814329825811L)
//  return 5;
//
//if(DNA_prefix.get() > 92814329825811L && DNA_prefix.get() <= 93383738944791L)
//  return 4;
//
//if(DNA_prefix.get() > 93383738944791L && DNA_prefix.get() <= 94146454918698L)
//  return 3;
//
//if(DNA_prefix.get() > 94146454918698L && DNA_prefix.get() <= 94803337066839L)
//  return 2;
//
//if(DNA_prefix.get() > 94803337066839L && DNA_prefix.get() <= 95162172495322L)
//  return 1;
//
//return 0;


//137G 128 reducers 23 chars
if(DNA_prefix.get() <= 3001707758326086L)
  return 63;

if(DNA_prefix.get() > 3001707758326086L && DNA_prefix.get() <= 3047176166009042L)
  return 62;

if(DNA_prefix.get() > 3047176166009042L && DNA_prefix.get() <= 3133766669490573L)
  return 61;

if(DNA_prefix.get() > 3133766669490573L && DNA_prefix.get() <= 3223036518152375L)
  return 60;

if(DNA_prefix.get() > 3223036518152375L && DNA_prefix.get() <= 3305415771056039L)
  return 59;

if(DNA_prefix.get() > 3305415771056039L && DNA_prefix.get() <= 3461345523927409L)
  return 58;

if(DNA_prefix.get() > 3461345523927409L && DNA_prefix.get() <= 3507956526484375L)
  return 57;

if(DNA_prefix.get() > 3507956526484375L && DNA_prefix.get() <= 3613735962677361L)
  return 56;

if(DNA_prefix.get() > 3613735962677361L && DNA_prefix.get() <= 3781579540967064L)
  return 55;

if(DNA_prefix.get() > 3781579540967064L && DNA_prefix.get() <= 3954510350351112L)
  return 54;

if(DNA_prefix.get() > 3954510350351112L && DNA_prefix.get() <= 4040732366145989L)
  return 53;

if(DNA_prefix.get() > 4040732366145989L && DNA_prefix.get() <= 4147622409108607L)
  return 52;

if(DNA_prefix.get() > 4147622409108607L && DNA_prefix.get() <= 4258218209947799L)
  return 51;

if(DNA_prefix.get() > 4258218209947799L && DNA_prefix.get() <= 4429817235146718L)
  return 50;

if(DNA_prefix.get() > 4429817235146718L && DNA_prefix.get() <= 4516599595607985L)
  return 49;

if(DNA_prefix.get() > 4516599595607985L && DNA_prefix.get() <= 4623406704663462L)
  return 48;

if(DNA_prefix.get() > 4623406704663462L && DNA_prefix.get() <= 4706072104053984L)
  return 47;

if(DNA_prefix.get() > 4706072104053984L && DNA_prefix.get() <= 4762644418863916L)
  return 46;

if(DNA_prefix.get() > 4762644418863916L && DNA_prefix.get() <= 5386348704412032L)
  return 45;

if(DNA_prefix.get() > 5386348704412032L && DNA_prefix.get() <= 5467423464177034L)
  return 44;

if(DNA_prefix.get() > 5467423464177034L && DNA_prefix.get() <= 5558013916015625L)
  return 43;

if(DNA_prefix.get() > 5558013916015625L && DNA_prefix.get() <= 5618743896484374L)
  return 42;

if(DNA_prefix.get() > 5618743896484374L && DNA_prefix.get() <= 5702721428661598L)
  return 41;

if(DNA_prefix.get() > 5702721428661598L && DNA_prefix.get() <= 5881114077133472L)
  return 40;

if(DNA_prefix.get() > 5881114077133472L && DNA_prefix.get() <= 5970001154882286L)
  return 39;

if(DNA_prefix.get() > 5970001154882286L && DNA_prefix.get() <= 6155178687227924L)
  return 38;

if(DNA_prefix.get() > 6155178687227924L && DNA_prefix.get() <= 6421508535124116L)
  return 37;

if(DNA_prefix.get() > 6421508535124116L && DNA_prefix.get() <= 6661650813261216L)
  return 36;

if(DNA_prefix.get() > 6661650813261216L && DNA_prefix.get() <= 6898722231128939L)
  return 35;

if(DNA_prefix.get() > 6898722231128939L && DNA_prefix.get() <= 6994108051562433L)
  return 34;

if(DNA_prefix.get() > 6994108051562433L && DNA_prefix.get() <= 7053367250667864L)
  return 33;

if(DNA_prefix.get() > 7053367250667864L && DNA_prefix.get() <= 7148742675781250L)
  return 32;

if(DNA_prefix.get() > 7148742675781250L && DNA_prefix.get() <= 7800819954477866L)
  return 31;

if(DNA_prefix.get() > 7800819954477866L && DNA_prefix.get() <= 7940142567910584L)
  return 30;

if(DNA_prefix.get() > 7940142567910584L && DNA_prefix.get() <= 8042717867103984L)
  return 29;

if(DNA_prefix.get() > 8042717867103984L && DNA_prefix.get() <= 8244704282420572L)
  return 28;

if(DNA_prefix.get() > 8244704282420572L && DNA_prefix.get() <= 8331083475576183L)
  return 27;

if(DNA_prefix.get() > 8331083475576183L && DNA_prefix.get() <= 8515472112843541L)
  return 26;

if(DNA_prefix.get() > 8515472112843541L && DNA_prefix.get() <= 8706280418925859L)
  return 25;

if(DNA_prefix.get() > 8706280418925859L && DNA_prefix.get() <= 8819547067187500L)
  return 24;

if(DNA_prefix.get() > 8819547067187500L && DNA_prefix.get() <= 8941642464056058L)
  return 23;

if(DNA_prefix.get() > 8941642464056058L && DNA_prefix.get() <= 9059906005859376L)
  return 22;

if(DNA_prefix.get() > 9059906005859376L && DNA_prefix.get() <= 9287489866639366L)
  return 21;

if(DNA_prefix.get() > 9287489866639366L && DNA_prefix.get() <= 9402923583984376L)
  return 20;

if(DNA_prefix.get() > 9402923583984376L && DNA_prefix.get() <= 9489186359919980L)
  return 19;

if(DNA_prefix.get() > 9489186359919980L && DNA_prefix.get() <= 10133330546437440L)
  return 18;

if(DNA_prefix.get() > 10133330546437440L && DNA_prefix.get() <= 10197654384184212L)
  return 17;

if(DNA_prefix.get() > 10197654384184212L && DNA_prefix.get() <= 10299302571870876L)
  return 16;

if(DNA_prefix.get() > 10299302571870876L && DNA_prefix.get() <= 10433178610335404L)
  return 15;

if(DNA_prefix.get() > 10433178610335404L && DNA_prefix.get() <= 10612411742140616L)
  return 14;

if(DNA_prefix.get() > 10612411742140616L && DNA_prefix.get() <= 10678859682134320L)
  return 13;

if(DNA_prefix.get() > 10678859682134320L && DNA_prefix.get() <= 10814099104395848L)
  return 12;

if(DNA_prefix.get() > 10814099104395848L && DNA_prefix.get() <= 10944099970256184L)
  return 11;

if(DNA_prefix.get() > 10944099970256184L && DNA_prefix.get() <= 11115770184527408L)
  return 10;

if(DNA_prefix.get() > 11115770184527408L && DNA_prefix.get() <= 11195280614428048L)
  return 9;

if(DNA_prefix.get() > 11195280614428048L && DNA_prefix.get() <= 11296623186166878L)
  return 8;

if(DNA_prefix.get() > 11296623186166878L && DNA_prefix.get() <= 11385570019530834L)
  return 7;

if(DNA_prefix.get() > 11385570019530834L && DNA_prefix.get() <= 11436149772238308L)
  return 6;

if(DNA_prefix.get() > 11436149772238308L && DNA_prefix.get() <= 11601791228226432L)
  return 5;

if(DNA_prefix.get() > 11601791228226432L && DNA_prefix.get() <= 11672967368098964L)
  return 4;

if(DNA_prefix.get() > 11672967368098964L && DNA_prefix.get() <= 11768306864837368L)
  return 3;

if(DNA_prefix.get() > 11768306864837368L && DNA_prefix.get() <= 11850417133354988L)
  return 2;

if(DNA_prefix.get() > 11850417133354988L && DNA_prefix.get() <= 11895271561915312L)
  return 1;

return 0;
    
  }
}

