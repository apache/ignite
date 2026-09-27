<?php
// $Id: function.set_null.php,v 1.1 2009/06/20 20:31:56 overall Exp $

/**
 * set_null Plugin
 *
 * Smarty function plugin to allow setting template variable to null
 *
 * Examples:
 * {set_null var="var_name"}
 *
 * @version  0.1
 * @author   Travis Cline <travis dot cline at gmail dot com>
 * @param    array
 * @param    Smarty
 * @return   string
 */

function sugar_function_set_null(&$smarty,$params)
{
    if (!isset($params['var'])) {
        trigger_error("set_null: missing 'var' parameter");
        return;
    }

    $smarty->set($params['var'], null);
}

?>
