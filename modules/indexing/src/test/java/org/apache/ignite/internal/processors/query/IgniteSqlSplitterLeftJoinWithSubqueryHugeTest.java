/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query;

import java.util.List;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests for correct distributed partitioned queries.
 */
public class IgniteSqlSplitterLeftJoinWithSubqueryHugeTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid("srv0", "src/test/config/ignite-join-subquery-huge.xml");
        startGrid("srv1", "src/test/config/ignite-join-subquery-huge.xml");

        Ignition.setClientMode(true);
        try {
            startGrid("cli0", "src/test/config/ignite-join-subquery-huge.xml");
        }
        finally {
            Ignition.setClientMode(false);
        }
    }

    /**
     *
     */
    public void test0() {
        String qry =
            "SELECT session_name, " +
                "       orig_ref, " +
                "       clsb_input_ref, " +
                "       common_ref, " +
                "       related_ref, " +
                "       clsb_match_ref, " +
                "       trade_dt, " +
                "       value_dt, " +
                "       last_action_tms, " +
                "       received_at, " +
                "       executed_at, " +
                "       tp_bic, " +
                "       orig_lei, " +
                "       orig_bic, " +
                "       cp_bic, " +
                "       tp_reference, " +
                "       cp_lei, " +
                "       fund_lei, " +
                "       curr_id_buy, " +
                "       vol_buy, " +
                "       curr_id_sell, " +
                "       vol_sell, " +
                "       exch_rate, " +
                "       input_status, " +
                "       exec_tms, " +
                "       tp_free_text, " +
                "       same_day_flag, " +
                "       report_juris, " +
                "       usi, " +
                "       prev_usi, " +
                "       report_juris2, " +
                "       usi2, " +
                "       prev_usi3, " +
                "       curr_susp_flag, " +
                "       cp_susp_flag, " +
                "       orig_susp_flag, " +
                "       fund_member_name, " +
                "       cp_fund_member_name, " +
                "       orgid, " +
                "       MT.msg_desc AS RejectionReason " +
                "FROM   (SELECT 'MAIN' AS SESSION_NAME, " +
                "               orig_ref, " +
                "               clsb_input_ref, " +
                "               common_ref, " +
                "               related_ref, " +
                "               clsb_match_ref, " +
                "               trade_dt, " +
                "               value_dt, " +
                "               ( Timestampadd('MILLISECOND', ( input_version_id ), last_action_tms) ) AS LAST_ACTION_TMS, " +
                "               exec_tms AS RECEIVED_AT, " +
                "               exec_tms AS EXECUTED_AT, " +
                "               tp_bic, " +
                "               orig_lei, " +
                "               orig_bic, " +
                "               cp_bic, " +
                "               tp_reference, " +
                "               cp_lei, " +
                "               fund_lei, " +
                "               curr_id_buy, " +
                "               vol_buy, " +
                "               curr_id_sell, " +
                "               vol_sell, " +
                "               exch_rate, " +
                "               input_status, " +
                "               exec_tms, " +
                "               tp_free_text, " +
                "               same_day_flag, " +
                "               report_juris, " +
                "               usi, " +
                "               prev_usi, " +
                "               report_juris2, " +
                "               usi2, " +
                "               prev_usi3, " +
                "               curr_susp_flag, " +
                "               cp_susp_flag, " +
                "               orig_susp_flag, " +
                "               COALESCE(bics.mbr_name, '') AS FUND_MEMBER_NAME, " +
                "               COALESCE(cpbics.mbr_name, '') AS CP_FUND_MEMBER_NAME, " +
                "               bl.orgid, " +
                "               COALESCE(MSG.msg_id, '') AS MSG_ID, " +
                "               COALESCE(MSG.msg_subid, '') AS MSG_SUBID, " +
                "               COALESCE(MSG.msg_type_id, 0) AS MSG_TYPE_ID " +
                "        FROM   \"TBCLSMInputHistoCache\".tbclsm_input_histo A " +
                "               INNER JOIN (SELECT DISTINCT orgid, cur_party_code " +
                "                           FROM \"DB2OrganisationsBICListCache\".organisations_bic_list " +
                "                           WHERE  orgid = 1) bl " +
                "                       ON ( a.tp_bic = bl.cur_party_code OR a.tp_reference = bl.cur_party_code ) " +
                "               LEFT JOIN (SELECT A.fund_id, " +
                "                                 a.mbr_name " +
                "                          FROM   \"TBCLSRTPFundCache\".tbclsr_tp_fund A " +
                "                                 INNER JOIN (SELECT fund_id, " +
                "                                                    Max(tp_id) TP_ID " +
                "                                             FROM " +
                "                                 \"TBCLSRTPFundCache\".tbclsr_tp_fund " +
                "                                             GROUP BY fund_id) B " +
                "                                         ON A.fund_id = B.fund_id " +
                "                                            AND A.tp_id = B.tp_id) bics " +
                "                      ON a.tp_bic = bics.fund_id " +
                "               LEFT JOIN (SELECT A.fund_id, " +
                "                                 a.mbr_name " +
                "                          FROM   \"TBCLSRTPFundCache\".tbclsr_tp_fund A " +
                "                                 INNER JOIN (SELECT fund_id, " +
                "                                                    Max(tp_id) TP_ID " +
                "                                             FROM \"TBCLSRTPFundCache\".tbclsr_tp_fund " +
                "                                             GROUP BY fund_id) B " +
                "                                         ON A.fund_id = B.fund_id AND A.tp_id = B.tp_id) cpbics " +
                "                      ON a.tp_reference = cpbics.fund_id " +
                "               LEFT JOIN (SELECT 13 TStatus, " +
                "                                 MSG.ntf_id, " +
                "                                 MSG.msg_id, " +
                "                                 MSG.msg_subid, " +
                "                                 MSG.msg_type_id " +
                "                          FROM   \"TBCLSRMessageCache\".tbclsr_message MSG " +
                "                          WHERE  MSG.msg_id IN ( 'RINP', 'RINS' )) Msg " +
                "                      ON Msg.ntf_id = A.ntf_id " +
                "                         AND A.input_status = Msg.tstatus " +
                "        WHERE clsb_input_ref = ' ') a " +
                "       LEFT JOIN \"TBDIMDMessageTypesCache\".tbdimd_message_types MT " +
                "              ON a.msg_subid = MT.msg_sub_id " +
                "                 AND MT.msg_type_id = a.msg_type_id " +
                "                 AND a.msg_id = MT.msg_id " +
                "                 AND MT.source_system = 'FX' ";

        printPlan(sql("EXPLAIN " + qry));

        sql(qry);
    }

    /**
     * @param plan Ignite query plan.
     */
    private void printPlan(List<List<?>> plan) {
        for (int i = 0; i < plan.size() - 1; ++i)
            System.out.println("MAP #" + i + ": " + plan.get(i).get(0));

        System.out.println("REDUCE: " + plan.get(plan.size() - 1).get(0));
    }

    /**
     * @param sql Query.
     * @return Result.
     */
    private List<List<?>> sql(String sql) {
        return grid("cli0").context().query().querySqlFields(
            new SqlFieldsQuery(sql), false).getAll();
    }
}
