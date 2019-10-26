package org.sheinbergon.nitrite.benchmark;

import lombok.Data;
import lombok.experimental.Accessors;
import org.dizitart.no2.objects.Id;

@Data
@Accessors(fluent = true, chain = true)
public class ArbitraryData {

    @Id
    private Integer id;

    private String text;

    private Double number1;

    private Double number2;

    private Integer index1;

    private Boolean flag1;

    private Boolean flag2;
}
