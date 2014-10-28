package org.apache.tajo.engine.function.randomlottery;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.*;
import org.apache.tajo.engine.function.AggFunction;
import org.apache.tajo.engine.function.FunctionContext;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.storage.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;


@Description(
    functionName = "lotto",
    description = "Hold a random lottery.",
    example = "> SELECT lotto(a, b [, c]);",
    returnType = TajoDataTypes.Type.TEXT,
    paramTypes = {
        @ParamTypes(paramTypes = {TajoDataTypes.Type.TEXT, TajoDataTypes.Type.TEXT}),
        @ParamTypes(paramTypes = {TajoDataTypes.Type.TEXT, TajoDataTypes.Type.TEXT, TajoDataTypes.Type.TEXT})
    }
)

public class RandomLottery extends AggFunction<Datum> {
    private static final Log LOG = LogFactory.getLog(RandomLottery.class.getName());
    public static final String DELIMITER = "/";

    public RandomLottery() {
        super(new Column[] {
            new Column("a", TajoDataTypes.Type.TEXT),
            new Column("b", TajoDataTypes.Type.TEXT),
            new Column("c", TajoDataTypes.Type.TEXT)    //optional
        });
    }

    @Override
    public FunctionContext newContext() {
        return new LotteryContext();
    }

    @Override
    public void eval(FunctionContext ctx, Tuple params) {
        LotteryContext lotteryContext = (LotteryContext)ctx;
        if(params.size()<2){
            lotteryContext.res = params.get(0).toString();
            return;
        }
        String ab = String.format("%s%s%s", params.get(0).toString().trim(), DELIMITER, params.get(1).toString().trim());
        if(params.size()>2){
            String c = params.get(2).toString();
            if(c.indexOf("@")>0){
                c = c.substring(0, c.indexOf("@"));
            }
            ab = String.format("%s%s%s", ab, DELIMITER, c);
        }
        LOG.debug(">>> ab: "+ ab);
        lotteryContext.list.add(ab);
    }

    @Override
    public Datum getPartialResult(FunctionContext ctx) {
        LotteryContext lotteryContext = (LotteryContext)ctx;
        int size = lotteryContext.list.size();
        LOG.debug(">>> size: "+ size);
        if(size>0) {
            Random random = new Random(System.nanoTime());
            int ptr = random.nextInt(size);
            lotteryContext.res = lotteryContext.list.get(ptr);
        }
        LOG.info(">>> getPartialResult:res: "+ lotteryContext.res);
        return DatumFactory.createText(lotteryContext.res);
    }

    @Override
    public DataType getPartialResultType() {
        return CatalogUtil.newSimpleDataType(Type.TEXT);
    }

    @Override
    public Datum terminate(FunctionContext ctx) {
        LotteryContext lotteryContext = (LotteryContext)ctx;
        LOG.info(">>> res: "+ lotteryContext.res);
        return DatumFactory.createText(lotteryContext.res);
    }

    private class LotteryContext implements FunctionContext {
        String res = "";
        List<String> list = new ArrayList<String>();
    }
}
