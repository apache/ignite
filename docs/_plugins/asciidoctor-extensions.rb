# MIT License
#
# Copyright (C) 2012-2020 Dan Allen, Sarah White, Ryan Waldron, and the
# individual contributors to Asciidoctor.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

require 'asciidoctor'
require 'asciidoctor/extensions'
require 'set'

include Asciidoctor

class TabsBlock < Asciidoctor::Extensions::BlockProcessor
  use_dsl

  named :tabs
  on_context :open
  parse_content_as :simple

  def render_tab(parent, name, options, tab_content, use_xml)
    if (options == 'unsupported')
        content = Asciidoctor.convert "[source]\n----\nThis API is not presently available for #{name}."+( use_xml ? " You can use XML configuration." : "")+"\n----", parent: parent.document
        return "<code-tab data-tab='#{name}' data-unavailable='true'>#{content}</code-tab>"
    else 
        if tab_content.empty?
          warn "There is an empty tab (#{name}) on the " + parent.document.attributes['doctitle'] + " page: " + parent.document.attributes['docfile']
         # File.write("log.txt", "There is an empty tab (#{name}) on the " + parent.document.attributes['doctitle'] + " page: " + parent.document.attributes['docfile'] + "\n", mode: "a")
        end
        content =  Asciidoctor.convert tab_content, parent: parent.document
        return "<code-tab data-tab='#{name}'>#{content}</code-tab>"
    end
  end


  def process parent, reader, attrs
    lines = reader.lines

    html = ''
    tab_content = ''
    name = ''
    options = ''
    tabs = Set.new
    lines.each do |line| 
       if (line =~ /^tab:.*\[.*\]/ ) 
          # render the previous tab if there is one
          unless name.empty?
              html = html + render_tab(parent, name, options, tab_content, tabs.include?("XML"))
          end

          tab_content = ''; 
          name = line[/tab:(.*)\[.*/,1] 
          tabs << name
          options = line[/tab:.*\[(.*)\]/,1] 
       else
          tab_content = tab_content + "\n" + line; 
       end  
    end 

    unless name.empty?
       html = html + render_tab(parent, name, options, tab_content, tabs.include?("XML"))
    end


    html = %(<code-tabs>#{html}</code-tabs>)

    create_pass_block parent, html, attrs
    
  end
end


Asciidoctor::Extensions.register do
  block TabsBlock
end


class JavadocUrlMacro < Extensions::InlineMacroProcessor
  use_dsl

  named :javadoc
  name_positional_attributes 'text'

  def process parent, target, attrs

    parts = target.split('.')

    if attrs['text'] == nil
      text = parts.last();
    else
      text = attrs['text'] 
    end

    target = parent.document.attributes['javadoc_base_url'] + '/' + parts.join('/') + ".html" 
    attrs.store('window', '_blank')

    (create_anchor parent, text, type: :link, target: target, attributes: attrs).render
  end
end

Asciidoctor::Extensions.register do
  inline_macro JavadocUrlMacro  
end
Extensions.register do 
 inline_macro do
   named :link
   parse_content_as :text

   process do |parent, target, attrs|
#     if(parent.document.attributes['latest'])
#      base_url = parent.document.attributes['base_url'] + '/latest' 
#     else
#      base_url = parent.document.attributes['base_url'] + '/' + parent.document.attributes['version'] 
#     end

#    print parent.document.attributes
    base_url = parent.document.attributes['base_url'] # + '/' + parent.document.attributes['version']
   
     if (text = attrs['text']).empty?
       text = target
     end

     if text =~ /(\^|, *window=_?blank *)$/
       text = text.sub(/\^$/,'')
       text = text.sub(/, *window=_?blank *$/,'')
       attrs.store('window', '_blank')
     end

     if target.start_with? 'http','ftp', '/', '#'
     else 
       target = base_url + '/' + %(#{target})
     end

     (create_anchor parent, text, type: :link, target: target, attributes: attrs).render
   end
 end
end

class ImageTreeProcessor < Extensions::Treeprocessor
  def process document

    image_width = (document.attr 'image_width', "")

    imagedir =    document.attributes['docdir'] 

    #scan for images
    (document.find_by context: :image).each do |img| 

        imagefile = imagedir + '/' + img.attributes['target']

       if !File.file?(imagefile) 
          warn 'Image does not exist: ' +imagefile 
       end

       if !(img.attributes['width'] || image_width.empty?)
           img.attributes['width'] = image_width
       end
    end
  end
end

Extensions.register do
  treeprocessor ImageTreeProcessor 
end
