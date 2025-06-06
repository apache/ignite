<?php

/*
 * Smarty plugin
 * -------------------------------------------------------------
 * Type:     function
 * Name:     assign_adv
 * File:     function.assign_adv.php
 * Version:  0.11
 * Purpose:  assigns smarty variables including arrays and range arrays
 * Author:   Bill Wheaton <billwheaton atsign mindspring fullstop com>
 * Synopsis:
 *      {assign_adv var="myvar" value="array('x','y',array('a'=>'abc'))"}
 *      or
 *      {assign_adv var="myvar" value="range(1,2)"}
 *      or
 *      {assign_adv var="myvar" value="myvalue"}
 *
 * Description: assign_adv is a direct and backward compatable replacement
 *  of assign.  It adds extra features, hence the '_adv' extention.
 *  The extra features are:
 *      value - can now contain a string formatted as a valid PHP array code or range code.
 *          the code is checked to see if it matches array(...) or range(...), and if so
 *          evaluates an array or range code from the contents of them (...).
 *
 * Examples:
 *  assign an array of hashes of javascript events (useful for html_field_group):
 *      {assign_adv
 *              var='events'
 *              value="array(
 *                      array(
 *                          'onfocus'=>'alert(\'Dia guit\');',
 *                          'onchange'=>'alert(\'Slainte\');'
 *                          ),
 *                      array(
 *                          'onfocus'=>'alert(\'God be with you\');',
 *                          'onchange'=>'alert(\'Cheers\');'
 *                          )
 *                      )" }
 * or assign a range of days to select for calendaring & scheduling
 *      {assign_adv var='repeatdays' value="range(1,30)" }
 *
 * Justification: Some might say "shoot, why not just write all your code in templates".  Well,
 *      I'm not really.  assign already assigns scalars, so allowing arrays and hashes seems
 *      logical.  I'm willing to draw the line there.
 *
 * Downside: Its slower to use assign_adv, so while you can use it as a replacement for
 *      assign, unless you need to assign an array, use assign instead.  assign_adv uses
 *      a PHP eval statement to facilitate it which can eat some time.
 *
 * See Also: function.assign.php
 *
 * ChangeLog: beta 0.10 first release (Bill Wheaton)
 *            beta 0.11 changed regular expression and flow control (Soeren Weber)
 *
 * COPYRIGHT:
 *     Copyright (c) 2003 Bill Wheaton
 *     This software is released under the GNU Lesser General Public License.
 *     Please read the following disclaimer
 *
 *      THIS SOFTWARE IS PROVIDED ''AS IS'' AND ANY EXPRESSED OR IMPLIED
 *      WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 *      OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 *      DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE
 *      LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY,
 *      OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
 *      OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 *      OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 *      LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 *      NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 *      SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 *     See the GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * -------------------------------------------------------------
 */
function sugar_function_assign_adv(&$smarty,$params)
{
    extract($params);

    if (empty($var)) {
        trigger_error("assign_adv: missing 'var' parameter");
        return;
    }

    if (!in_array('value', array_keys($params))) {
        trigger_error("assign_adv: missing 'value' parameter");
        return;
    }
    if (preg_match('/^\s*array\s*\(\s*(.*)\s*\)\s*$/s',$value,$match)){
        eval('$value=array('.str_replace("\n", "", $match[1]).');');
    }
    else if (preg_match('/^\s*range\s*\(\s*(.*)\s*\)\s*$/s',$value,$match)){
        eval('$value=range('.str_replace("\n", "", $match[1]).');');
    }

    $smarty->set($var, $value);
}
?>
