import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class BioPartitioner extends Partitioner<LongWritable,LongWritable> {
  @Override
  //public int getPartition(IntWritable DNA_prefix, LongWritable value, int numReduceTasks) {
  public int getPartition(LongWritable DNA_prefix, LongWritable value, int numReduceTasks) {
    //64G 32 reducers
    if(DNA_prefix.get() <= 3041273910077206L)
      return 31;
    
    if(DNA_prefix.get() > 3041273910077206L && DNA_prefix.get() <= 3198240172804687L)
      return 30;
    
    if(DNA_prefix.get() > 3198240172804687L && DNA_prefix.get() <= 3337004975378906L)
      return 29;
    
    if(DNA_prefix.get() > 3337004975378906L && DNA_prefix.get() <= 3562895184764934L)
      return 28;
    
    if(DNA_prefix.get() > 3562895184764934L && DNA_prefix.get() <= 3934431995993469L)
      return 27;
    
    if(DNA_prefix.get() > 3934431995993469L && DNA_prefix.get() <= 4096731682140232L)
      return 26;
    
    if(DNA_prefix.get() > 4096731682140232L && DNA_prefix.get() <= 4289245473560161L)
      return 25;
    
    if(DNA_prefix.get() > 4289245473560161L && DNA_prefix.get() <= 4570991518105824L)
      return 24;
    
    if(DNA_prefix.get() > 4570991518105824L && DNA_prefix.get() <= 4729382324218750L)
      return 23;
    
    if(DNA_prefix.get() > 4729382324218750L && DNA_prefix.get() <= 5422119079660159L)
      return 22;
    
    if(DNA_prefix.get() > 5422119079660159L && DNA_prefix.get() <= 5583149730371709L)
      return 21;
    
    if(DNA_prefix.get() > 5583149730371709L && DNA_prefix.get() <= 5721569335842731L)
      return 20;
    
    if(DNA_prefix.get() > 5721569335842731L && DNA_prefix.get() <= 6133865956052421L)
      return 19;
    
    if(DNA_prefix.get() > 6133865956052421L && DNA_prefix.get() <= 6799143637214282L)
      return 18;
    
    if(DNA_prefix.get() > 6799143637214282L && DNA_prefix.get() <= 7006823725576097L)
      return 17;
    
    if(DNA_prefix.get() > 7006823725576097L && DNA_prefix.get() <= 7152239349609372L)
      return 16;
    
    if(DNA_prefix.get() > 7152239349609372L && DNA_prefix.get() <= 7939745722372822L)
      return 15;
    
    if(DNA_prefix.get() > 7939745722372822L && DNA_prefix.get() <= 8237731560493416L)
      return 14;
    
    if(DNA_prefix.get() > 8237731560493416L && DNA_prefix.get() <= 8550003581367049L)
      return 13;
    
    if(DNA_prefix.get() > 8550003581367049L && DNA_prefix.get() <= 8894030192290000L)
      return 12;
    
    if(DNA_prefix.get() > 8894030192290000L && DNA_prefix.get() <= 9246793569140192L)
      return 11;
    
    if(DNA_prefix.get() > 9246793569140192L && DNA_prefix.get() <= 9441375732421876L)
      return 10;
    
    if(DNA_prefix.get() > 9441375732421876L && DNA_prefix.get() <= 10164565396397794L)
      return 9;
    
    if(DNA_prefix.get() > 10164565396397794L && DNA_prefix.get() <= 10390927714434296L)
      return 8;
    
    if(DNA_prefix.get() > 10390927714434296L && DNA_prefix.get() <= 10649868793717084L)
      return 7;
    
    if(DNA_prefix.get() > 10649868793717084L && DNA_prefix.get() <= 10918971675743740L)
      return 6;
    
    if(DNA_prefix.get() > 10918971675743740L && DNA_prefix.get() <= 11145719554410832L)
      return 5;
    
    if(DNA_prefix.get() > 11145719554410832L && DNA_prefix.get() <= 11329650878906252L)
      return 4;
    
    if(DNA_prefix.get() > 11329650878906252L && DNA_prefix.get() <= 11444091796875000L)
      return 3;
    
    if(DNA_prefix.get() > 11444091796875000L && DNA_prefix.get() <= 11691246727061672L)
      return 2;
    
    if(DNA_prefix.get() > 11691246727061672L && DNA_prefix.get() <= 11857443089585368L)
      return 1;
    
    return 0;
    //137G 64 reducers 23 chars
    //if(DNA_prefix.get() <= 3001707758326086L)
    //  return 63;
    //
    //if(DNA_prefix.get() > 3001707758326086L && DNA_prefix.get() <= 3047176166009042L)
    //  return 62;
    //
    //if(DNA_prefix.get() > 3047176166009042L && DNA_prefix.get() <= 3133766669490573L)
    //  return 61;
    //
    //if(DNA_prefix.get() > 3133766669490573L && DNA_prefix.get() <= 3223036518152375L)
    //  return 60;
    //
    //if(DNA_prefix.get() > 3223036518152375L && DNA_prefix.get() <= 3305415771056039L)
    //  return 59;
    //
    //if(DNA_prefix.get() > 3305415771056039L && DNA_prefix.get() <= 3461345523927409L)
    //  return 58;
    //
    //if(DNA_prefix.get() > 3461345523927409L && DNA_prefix.get() <= 3507956526484375L)
    //  return 57;
    //
    //if(DNA_prefix.get() > 3507956526484375L && DNA_prefix.get() <= 3613735962677361L)
    //  return 56;
    //
    //if(DNA_prefix.get() > 3613735962677361L && DNA_prefix.get() <= 3781579540967064L)
    //  return 55;
    //
    //if(DNA_prefix.get() > 3781579540967064L && DNA_prefix.get() <= 3954510350351112L)
    //  return 54;
    //
    //if(DNA_prefix.get() > 3954510350351112L && DNA_prefix.get() <= 4040732366145989L)
    //  return 53;
    //
    //if(DNA_prefix.get() > 4040732366145989L && DNA_prefix.get() <= 4147622409108607L)
    //  return 52;
    //
    //if(DNA_prefix.get() > 4147622409108607L && DNA_prefix.get() <= 4258218209947799L)
    //  return 51;
    //
    //if(DNA_prefix.get() > 4258218209947799L && DNA_prefix.get() <= 4429817235146718L)
    //  return 50;
    //
    //if(DNA_prefix.get() > 4429817235146718L && DNA_prefix.get() <= 4516599595607985L)
    //  return 49;
    //
    //if(DNA_prefix.get() > 4516599595607985L && DNA_prefix.get() <= 4623406704663462L)
    //  return 48;
    //
    //if(DNA_prefix.get() > 4623406704663462L && DNA_prefix.get() <= 4706072104053984L)
    //  return 47;
    //
    //if(DNA_prefix.get() > 4706072104053984L && DNA_prefix.get() <= 4762644418863916L)
    //  return 46;
    //
    //if(DNA_prefix.get() > 4762644418863916L && DNA_prefix.get() <= 5386348704412032L)
    //  return 45;
    //
    //if(DNA_prefix.get() > 5386348704412032L && DNA_prefix.get() <= 5467423464177034L)
    //  return 44;
    //
    //if(DNA_prefix.get() > 5467423464177034L && DNA_prefix.get() <= 5558013916015625L)
    //  return 43;
    //
    //if(DNA_prefix.get() > 5558013916015625L && DNA_prefix.get() <= 5618743896484374L)
    //  return 42;
    //
    //if(DNA_prefix.get() > 5618743896484374L && DNA_prefix.get() <= 5702721428661598L)
    //  return 41;
    //
    //if(DNA_prefix.get() > 5702721428661598L && DNA_prefix.get() <= 5881114077133472L)
    //  return 40;
    //
    //if(DNA_prefix.get() > 5881114077133472L && DNA_prefix.get() <= 5970001154882286L)
    //  return 39;
    //
    //if(DNA_prefix.get() > 5970001154882286L && DNA_prefix.get() <= 6155178687227924L)
    //  return 38;
    //
    //if(DNA_prefix.get() > 6155178687227924L && DNA_prefix.get() <= 6421508535124116L)
    //  return 37;
    //
    //if(DNA_prefix.get() > 6421508535124116L && DNA_prefix.get() <= 6661650813261216L)
    //  return 36;
    //
    //if(DNA_prefix.get() > 6661650813261216L && DNA_prefix.get() <= 6898722231128939L)
    //  return 35;
    //
    //if(DNA_prefix.get() > 6898722231128939L && DNA_prefix.get() <= 6994108051562433L)
    //  return 34;
    //
    //if(DNA_prefix.get() > 6994108051562433L && DNA_prefix.get() <= 7053367250667864L)
    //  return 33;
    //
    //if(DNA_prefix.get() > 7053367250667864L && DNA_prefix.get() <= 7148742675781250L)
    //  return 32;
    //
    //if(DNA_prefix.get() > 7148742675781250L && DNA_prefix.get() <= 7800819954477866L)
    //  return 31;
    //
    //if(DNA_prefix.get() > 7800819954477866L && DNA_prefix.get() <= 7940142567910584L)
    //  return 30;
    //
    //if(DNA_prefix.get() > 7940142567910584L && DNA_prefix.get() <= 8042717867103984L)
    //  return 29;
    //
    //if(DNA_prefix.get() > 8042717867103984L && DNA_prefix.get() <= 8244704282420572L)
    //  return 28;
    //
    //if(DNA_prefix.get() > 8244704282420572L && DNA_prefix.get() <= 8331083475576183L)
    //  return 27;
    //
    //if(DNA_prefix.get() > 8331083475576183L && DNA_prefix.get() <= 8515472112843541L)
    //  return 26;
    //
    //if(DNA_prefix.get() > 8515472112843541L && DNA_prefix.get() <= 8706280418925859L)
    //  return 25;
    //
    //if(DNA_prefix.get() > 8706280418925859L && DNA_prefix.get() <= 8819547067187500L)
    //  return 24;
    //
    //if(DNA_prefix.get() > 8819547067187500L && DNA_prefix.get() <= 8941642464056058L)
    //  return 23;
    //
    //if(DNA_prefix.get() > 8941642464056058L && DNA_prefix.get() <= 9059906005859376L)
    //  return 22;
    //
    //if(DNA_prefix.get() > 9059906005859376L && DNA_prefix.get() <= 9287489866639366L)
    //  return 21;
    //
    //if(DNA_prefix.get() > 9287489866639366L && DNA_prefix.get() <= 9402923583984376L)
    //  return 20;
    //
    //if(DNA_prefix.get() > 9402923583984376L && DNA_prefix.get() <= 9489186359919980L)
    //  return 19;
    //
    //if(DNA_prefix.get() > 9489186359919980L && DNA_prefix.get() <= 10133330546437440L)
    //  return 18;
    //
    //if(DNA_prefix.get() > 10133330546437440L && DNA_prefix.get() <= 10197654384184212L)
    //  return 17;
    //
    //if(DNA_prefix.get() > 10197654384184212L && DNA_prefix.get() <= 10299302571870876L)
    //  return 16;
    //
    //if(DNA_prefix.get() > 10299302571870876L && DNA_prefix.get() <= 10433178610335404L)
    //  return 15;
    //
    //if(DNA_prefix.get() > 10433178610335404L && DNA_prefix.get() <= 10612411742140616L)
    //  return 14;
    //
    //if(DNA_prefix.get() > 10612411742140616L && DNA_prefix.get() <= 10678859682134320L)
    //  return 13;
    //
    //if(DNA_prefix.get() > 10678859682134320L && DNA_prefix.get() <= 10814099104395848L)
    //  return 12;
    //
    //if(DNA_prefix.get() > 10814099104395848L && DNA_prefix.get() <= 10944099970256184L)
    //  return 11;
    //
    //if(DNA_prefix.get() > 10944099970256184L && DNA_prefix.get() <= 11115770184527408L)
    //  return 10;
    //
    //if(DNA_prefix.get() > 11115770184527408L && DNA_prefix.get() <= 11195280614428048L)
    //  return 9;
    //
    //if(DNA_prefix.get() > 11195280614428048L && DNA_prefix.get() <= 11296623186166878L)
    //  return 8;
    //
    //if(DNA_prefix.get() > 11296623186166878L && DNA_prefix.get() <= 11385570019530834L)
    //  return 7;
    //
    //if(DNA_prefix.get() > 11385570019530834L && DNA_prefix.get() <= 11436149772238308L)
    //  return 6;
    //
    //if(DNA_prefix.get() > 11436149772238308L && DNA_prefix.get() <= 11601791228226432L)
    //  return 5;
    //
    //if(DNA_prefix.get() > 11601791228226432L && DNA_prefix.get() <= 11672967368098964L)
    //  return 4;
    //
    //if(DNA_prefix.get() > 11672967368098964L && DNA_prefix.get() <= 11768306864837368L)
    //  return 3;
    //
    //if(DNA_prefix.get() > 11768306864837368L && DNA_prefix.get() <= 11850417133354988L)
    //  return 2;
    //
    //if(DNA_prefix.get() > 11850417133354988L && DNA_prefix.get() <= 11895271561915312L)
    //  return 1;
    //
    //return 0;

   
    //137G 48 reducers 23 chars
    //if(DNA_prefix.get() <= 3020304329183432L)
    //  return 47;
    //
    //if(DNA_prefix.get() > 3020304329183432L && DNA_prefix.get() <= 3096969416583723L)
    //  return 46;
    //
    //if(DNA_prefix.get() > 3096969416583723L && DNA_prefix.get() <= 3229412839757063L)
    //  return 45;
    //
    //if(DNA_prefix.get() > 3229412839757063L && DNA_prefix.get() <= 3323669090389873L)
    //  return 44;
    //
    //if(DNA_prefix.get() > 3323669090389873L && DNA_prefix.get() <= 3496000608338711L)
    //  return 43;
    //
    //if(DNA_prefix.get() > 3496000608338711L && DNA_prefix.get() <= 3616291890624622L)
    //  return 42;
    //
    //if(DNA_prefix.get() > 3616291890624622L && DNA_prefix.get() <= 3802866942771824L)
    //  return 41;
    //
    //if(DNA_prefix.get() > 3802866942771824L && DNA_prefix.get() <= 4004161936557342L)
    //  return 40;
    //
    //if(DNA_prefix.get() > 4004161936557342L && DNA_prefix.get() <= 4148790897851673L)
    //  return 39;
    //
    //if(DNA_prefix.get() > 4148790897851673L && DNA_prefix.get() <= 4281511903249357L)
    //  return 38;
    //
    //if(DNA_prefix.get() > 4281511903249357L && DNA_prefix.get() <= 4480435047997994L)
    //  return 37;
    //
    //if(DNA_prefix.get() > 4480435047997994L && DNA_prefix.get() <= 4629281479086586L)
    //  return 36;
    //
    //if(DNA_prefix.get() > 4629281479086586L && DNA_prefix.get() <= 4730020014367036L)
    //  return 35;
    //
    //if(DNA_prefix.get() > 4730020014367036L && DNA_prefix.get() <= 5372891340730238L)
    //  return 34;
    //
    //if(DNA_prefix.get() > 5372891340730238L && DNA_prefix.get() <= 5469404865758343L)
    //  return 33;
    //
    //if(DNA_prefix.get() > 5469404865758343L && DNA_prefix.get() <= 5580431874201848L)
    //  return 32;
    //
    //if(DNA_prefix.get() > 5580431874201848L && DNA_prefix.get() <= 5689544574115434L)
    //  return 31;
    //
    //if(DNA_prefix.get() > 5689544574115434L && DNA_prefix.get() <= 5887252574895193L)
    //  return 30;
    //
    //if(DNA_prefix.get() > 5887252574895193L && DNA_prefix.get() <= 6055282386245969L)
    //  return 29;
    //
    //if(DNA_prefix.get() > 6055282386245969L && DNA_prefix.get() <= 6351051365019961L)
    //  return 28;
    //
    //if(DNA_prefix.get() > 6351051365019961L && DNA_prefix.get() <= 6673697443857050L)
    //  return 27;
    //
    //if(DNA_prefix.get() > 6673697443857050L && DNA_prefix.get() <= 6933282368899587L)
    //  return 26;
    //
    //if(DNA_prefix.get() > 6933282368899587L && DNA_prefix.get() <= 7042947995055786L)
    //  return 25;
    //
    //if(DNA_prefix.get() > 7042947995055786L && DNA_prefix.get() <= 7150835873546875L)
    //  return 24;
    //
    //if(DNA_prefix.get() > 7150835873546875L && DNA_prefix.get() <= 7847944892578125L)
    //  return 23;
    //
    //if(DNA_prefix.get() > 7847944892578125L && DNA_prefix.get() <= 7991249506437456L)
    //  return 22;
    //
    //if(DNA_prefix.get() > 7991249506437456L && DNA_prefix.get() <= 8246142260428982L)
    //  return 21;
    //
    //if(DNA_prefix.get() > 8246142260428982L && DNA_prefix.get() <= 8383146948312500L)
    //  return 20;
    //
    //if(DNA_prefix.get() > 8383146948312500L && DNA_prefix.get() <= 8573839905696617L)
    //  return 19;
    //
    //if(DNA_prefix.get() > 8573839905696617L && DNA_prefix.get() <= 8828994608730439L)
    //  return 18;
    //
    //if(DNA_prefix.get() > 8828994608730439L && DNA_prefix.get() <= 9007476198234238L)
    //  return 17;
    //
    //if(DNA_prefix.get() > 9007476198234238L && DNA_prefix.get() <= 9246565789558536L)
    //  return 16;
    //
    //if(DNA_prefix.get() > 9246565789558536L && DNA_prefix.get() <= 9409910583339594L)
    //  return 15;
    //
    //if(DNA_prefix.get() > 9409910583339594L && DNA_prefix.get() <= 9524307350957060L)
    //  return 14;
    //
    //if(DNA_prefix.get() > 9524307350957060L && DNA_prefix.get() <= 10176506219274712L)
    //  return 13;
    //
    //if(DNA_prefix.get() > 10176506219274712L && DNA_prefix.get() <= 10326304148042862L)
    //  return 12;
    //
    //if(DNA_prefix.get() > 10326304148042862L && DNA_prefix.get() <= 10470061028113526L)
    //  return 11;
    //
    //if(DNA_prefix.get() > 10470061028113526L && DNA_prefix.get() <= 10660546531757480L)
    //  return 10;
    //
    //if(DNA_prefix.get() > 10660546531757480L && DNA_prefix.get() <= 10830924462812472L)
    //  return 9;
    //
    //if(DNA_prefix.get() > 10830924462812472L && DNA_prefix.get() <= 10967095876796172L)
    //  return 8;
    //
    //if(DNA_prefix.get() > 10967095876796172L && DNA_prefix.get() <= 11185998288897740L)
    //  return 7;
    //
    //if(DNA_prefix.get() > 11185998288897740L && DNA_prefix.get() <= 11298785055605920L)
    //  return 6;
    //
    //if(DNA_prefix.get() > 11298785055605920L && DNA_prefix.get() <= 11412047554083984L)
    //  return 5;
    //
    //if(DNA_prefix.get() > 11412047554083984L && DNA_prefix.get() <= 11577148437499998L)
    //  return 4;
    //
    //if(DNA_prefix.get() > 11577148437499998L && DNA_prefix.get() <= 11678566637287060L)
    //  return 3;
    //
    //if(DNA_prefix.get() > 11678566637287060L && DNA_prefix.get() <= 11796492162020600L)
    //  return 2;
    //
    //if(DNA_prefix.get() > 11796492162020600L && DNA_prefix.get() <= 11880337222396664L)
    //  return 1;
    //
    //return 0;
  }
}

