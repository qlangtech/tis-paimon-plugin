package com.qlangtech.tis.plugin.paimon.datax.hook;

import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.exec.IExecChainContext;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.paimon.datax.DataxPaimonWriter;
import com.qlangtech.tis.plugin.paimon.datax.PaimonSelectedTab;
import com.qlangtech.tis.plugin.paimon.datax.test.PaimonTestUtils;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.*;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-17 15:27
 **/
public class TestPreExecutor {

    @Test
    public void getRecordTransformerRules() {

        IExecChainContext execContext = EasyMock.mock("execContext", IExecChainContext.class);
        EasyMock.expect(execContext.getProcessor()).andReturn(DataxProcessor.load(null, "mysql_paimon")).anyTimes();
        final DataxPaimonWriter writer = PaimonTestUtils.getPaimonWriter();

        PaimonSelectedTab tab = new PaimonSelectedTab();
        tab.name = "full_types";

        PreExecutor preExecutor = new PreExecutor(execContext, writer, writer.parseEntity(tab), tab);

        EasyMock.replay(execContext);
        Optional<RecordTransformerRules> recordTransformerRules = preExecutor.getRecordTransformerRules();
        Assert.assertTrue(recordTransformerRules.isPresent());

        EasyMock.verify(execContext);
    }
}