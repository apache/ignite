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
    base_url = parent.document.attributes['base_url'] + '/' + parent.document.attributes['version']
   
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

    #scan for images
    (document.find_by context: :image).each do |img| 
      if  !(img.attributes['width'] || image_width.empty?)
           img.attributes['width'] = image_width
       end
    end
  end
end

Extensions.register do
  treeprocessor ImageTreeProcessor 
end
