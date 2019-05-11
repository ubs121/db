db.nodes_tmp.find({}).forEach( function(obj) {
 var tags = [];

 for(var p in obj){
  if (typeof obj[p] == "string") {
    var parts=obj[p].split(/[\s,\\;]+/);
    
    for (var i = 0; i < parts.length; i++) {
       if (parts[i].length>2 && tags.indexOf(parts[i]) < 0 ) {
        tags.push(parts[i]);
       }
    }

  }
 }
 
 obj.tags = tags;
 
 db.nodes_tmp.save(obj); 
});