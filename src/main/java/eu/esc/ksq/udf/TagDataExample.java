package eu.esc.ksq.udf;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;

@UdfDescription(name = "Simple Tagger", description = "A simple example to practice the lifecycle")
public class TagDataExample {

    @Udf(description = "checks input and adds a tag")
    public String tagAmount(@UdfParameter("amount") final int amount) {
        return Tag.fromAmount(amount).name();
    }
    
    static enum Tag {
        LOW, MED, HIGH;

        public static Tag fromAmount(int amount) {
            if (amount <= 5) {
                return LOW;
            } else if (amount <= 10) {
                return MED;
            } else return HIGH;
        }
    }
}
