PieChart = (function() {
  function PieChart(target, data) {
    var width = 250,
        height = 250,
        radius = Math.min(width, height) / 2;

    var color = d3.scale.category10()

    var arc = d3.svg.arc()
        .outerRadius(radius - 10)
        .innerRadius(0);

    var pie = d3.layout.pie()
        .sort(null)
        .value(function(d) { return d.count; });

    var svg = d3.select(target).append("svg")
        .attr("width", width)
        .attr("height", height)
        .append("g")
        .attr("transform", "translate(" + width / 2 + "," + height / 2 + ")");

    var g = svg.selectAll(".arc")
        .data(pie(data))
        .enter().append("g")
        .attr("class", "arc");

    g.append("path")
     .attr("d", arc)
     .style("fill", function(d) { return color(d.data.type); });

    var legend = d3.select(target).append("svg")
      .attr("class", "legend")
      .attr("width", radius + 150)
      .attr("height", 50)
      .selectAll("g")
      .data(data)
      .enter().append("g")
      .attr("transform", function(d, i) { return "translate(40," + i * 20 + ")"; });

    legend.append("rect")
      .attr("width", 18)
      .attr("height", 18)
      .style("fill", function(d) { return color(d.type); });

    legend.append("text")
      .attr("x", 24)
      .attr("y", 9)
      .attr("dy", ".35em")
      .text(function(d) { return d.type; });
  }

  return PieChart

})();

$(document).ready(function() {

  var data = [{type: "Metadaten unversehrt", count: nimbus.linkcheckerWorking},
              {type: "Metadaten mit toten Links", count: nimbus.linkcheckerBroken}];

  pieChart = new PieChart("#linkchecker-pie-chart", data);

  // Add Sorting to tables
  $('table').each(function() {
    var tablecontainer = $(this).parent('div')[0];

    if($(this).find('button.sort').length === 0) {
      return; // only invoke sortscript when there are sort buttons
    }

    // get value-names by their sort-buttons
    var valueNames = [];
    $(this).find('.sort').each(function() {
      valueNames.push($(this).data('sort'));
    });

    var list = new List(tablecontainer, {valueNames: valueNames});

    // add handler to recalculate the sum of dead links
    if(this.className.indexOf('has-sum') > -1) {
      list.on('updated', function(event) {
        var sum = 0;
        event.visibleItems.forEach(function(item) {
          var values = item.values();
          sum += parseInt(values.brokenrecords);
        })

        $('#sumofdeadlinks').html(sum);
      });

    }
  });
  //var a = new List($('table.overview')[0], {valueNames: ['datasource', 'brokenrecords']});
});
