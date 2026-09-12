$Id: README.txt,v 1.1 2009/06/20 20:31:56 overall Exp $

*** IMPORTANT NOTE - DRUPAL v6 BETA ***
If you are using this with Drupal v.6 Beta 2 or older you must apply the following patch in
includes/theme.inc function drupal_find_theme_templates (see http://drupal.org/node/184876):

REPLACE
    $template = substr($template, 0, strpos($template, '.'));
WITH
    if (strpos($template, '.') !== FALSE) {
      $template = substr($template, 0, strpos($template, '.'));
    }
END

This Theme Engine for Drupal allows you to use Smarty templates in a fully integrated and
seamless fashion. It expects a Smarty installation in the ./libs directory - you must obtain
this and extract it here yourself. It is not included by default as a policy of the Drupal.org
contributions repository.

Since Drupal 5, and even more so for Drupal 6, this template engine has become technically much 
simpler because of the improvements in Drupal's own themeing/templating system.

The 'default' theme for this engine is 'box_grey_smarty', but this is not yet updated for Drupal 6.

Alternate downloads: http://download.berlios.de/drupal-smarty/
    -   There is no package for v5 or v6 yet at berlios.de
    -   Find the appropriate download here and skip steps two and three.
    -   These links include the Smarty Template Engine.

Installation:
    1.  Extract tarball in (drupal_base)/sites/all/themes/engines/
    2.  Extract Smarty Template Engine tarball from http://smarty.php.net/download.php
        to a temporary location.
    3.  Copy the 'libs' subdirectory from the temporary location to
        (drupal_base)/sites/all/themes/engines/smarty/libs
    4.  Check that Smarty.class.php is located at
        (drupal_base)/themes/engines/smarty/libs/Smarty.class.php
    5.  Ensure (drupal_base)/themes/engines/smarty/templates_c directory is readable and writable by
        the web server process (you might have to give it permission 777).

Notes:
	-   You can put plugins in the (drupal_base)/themes/engines/smarty/plugins directory. If you
        download the full package from http://download.berlios.de/drupal-smarty/ this directory
        will already include the following useful plugins:
            -   Smarty plugin assign_adv from http://smarty.incutio.com/?page=AdvancedAssignPlugin
    -   There might be a problem with clashes with functions called smarty_foo (Drupal might
        discover them as potential theme_foo functions) so try and avoid this.
    -   The way Smarty plugins/'wrapper' functions work has changed. Create a function
        mytheme__register_smarty_functions in your theme's template.php which returns an array
        like the array $plugins in function &_smarty_get_object
    -   smartytemplate.php is no longer used. Put a template.php file in your theme instead

Help/Problems:
	For problems or help:
	First search drupal.org for problems and look through drupal.org forums before
	contacting through email.

	Author email: djnz at 
    travis dot cline at gmail dot com
	Original author email: lobo at yahoo dot com

	Please include 'drupal smarty engine question' or something of the sort in the subject.

Patches:
	Please feel free to email any patches or improvements.

Contributors:
	lobo
	nsanity
	tacker
	Goba
	tclineks
    djnz

License:
	Distributed under GPL - see LICENSE.txt

// TODO theme building documentation
// - note changes for Drupal 6


Template variables available
----------------------------
  $layout           - none, left, right or both
  $id               - count for the number of times the hook has been called
  $zebra            - odd/even zebra count for the number of times the hook has been called

  $directory        - path to the current theme
  $is_front         - front page flag
  $logged_in        - TRUE if not anonymous
  $is_admin         = TRUE if admin user
  $head_title       - title for <title> tag
  $base_pat         - base url of site
  $breadcrumb       - themed breadcrumbs
  $feed_icons
  $footer_message   - from site information
  $head             - other stuff for <head>
  $language         - language object
  $help
  $logo
  $messages
  $primary_links    - array of primary links
  $secondary_links  - array of secondary links
  $search_box       - themed search box
  $site_name        
  $mission
  $site_slogan
  $help
  $logo
  $messages

  $css              - ? css         ) Not sure what the difference
  $styles           - css files     ) is here?
  $scripts          - javascript to load in <head>
  $tabs             - themed tasks
  $title            - page title (for html title tag)
  $closure          - theme('closure');

  $body_classes     - for insertion in <body$body_classes>

  $template_files   - an array of potential template files - not much use to a template?

For node.tpl
  $node             - the raw node object
  $teaser           - TRUE if the content is a teaser (?? is this right?)
  $content          - the $node->teaser or $node->body as appropriate
  $date             - formatted creation date
  $links            - themed links
  $name             - themed user name of creator
  $node_url         - the url to display the node
  $taxonomy         - taxonomy links
  $terms            - themed taxonomy links
  $title            - the node title (plain text, so not sure how &,',? etc. are treated)
  $submitted        - themed submission user/time
  $picture          - themed avatar

  $...              - an explosion of the $node[...] array - lets hope there are no clashes!

For block.tpl
  $block_zebra      - 'odd' or 'even' for instances of a block in a region
  $block_id         - counter for block in a region
  $block            - the block object

Note that the smarty template engine no longer sets the following additional variables:
  $user             - use {$smarty.globals.user} if you must, or {
  $path_to_theme    - this is now replaced by {$directory} which is set by Drupal core
  $base_path        - but this can still be used as it is now set by Drupal core
