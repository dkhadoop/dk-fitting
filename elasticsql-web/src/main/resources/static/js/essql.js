function getConf(){
    $.ajax({
        type: "GET",
        url: 'es_conf',
        dataType: 'json',
        cache: false,
        async: false,
        success: function (result) {
            $("#es_url").val(result.input1);
            $("#esCluster_name").val(result.input2);
            $("#esIndex_name").val(result.input3);
        },
        error: function (error) {
            showTips("出现未知错误！");
        }
    });
}

function query(){
    var sqlContent = $("#sqlContent").val();
    $("#returndata").empty();
    $("#returndata").html("查询中......");
    var arrbuffer = [];
    $.ajax({
        type: "POST",
        data: {
            "sql": sqlContent
        },
        url: 'es',
        dataType: 'json',
        cache: false,
        async: false,
        success: function (result) {
            var size = result.esdata.length;
            for(var i =0;i<size;i++){
                arrbuffer.push(result.esdata[i]+"<br/>")
            }
            $("#returndata").html(arrbuffer);
            if(result.saveSucceed.isWrite){
                $("#exportResult").attr("href","result_file");
            }else {
                $("#exportResult").attr("href","#");
            }
        },
        error: function (error) {
            $("#returndata").html("出现未知错误！");
        }
    });
}

function saveSchema(){
    $("#es_url").validate2("连接es的ip和端口");
    $("#esCluster_name").validate2("es集群名称");
    $("#esIndex_name").validate2("es索引名称");
    var es_url =  $("#es_url").val();
    var esCluster_name = $("#esCluster_name").val();
    var esIndex_name = $("#esIndex_name").val();
    //var schemaContent = $("#inputSchema").val();
    $.ajax({
        type: "POST",
        data: {
            "es_url": es_url,
            "esCluster_name":esCluster_name,
            "esIndex_name":esIndex_name
        },
        url: 'schema',
        dataType: 'json',
        cache: false,
        async: false,
        success: function (result) {
            if (result.succeed){
                alert("保存成功!");
            }else {
                alert(result.msg);
            }
        },
        error: function (error) {
            //$("#returndata").html("出现未知错误！请查看schema格式是否正确！");
            alert("出现未知错误！请查看输入信息是否正确以及集群是否启动！");
        }
    });

}

function helpModule(){
    alert("1.修改schema中的信息并保存 \n2.输入sql语句 \n3.查询结果");
}

//判断输入框内容是否为空
$.fn.validate2 = function(tips){
    if($(this).val() == "" || $.trim($(this).val()).length == 0){
        showTips(tips + "不能为空！");
        throw SyntaxError(); //如果验证不通过，则不执行后面
    }
}

function showTips(txt,time,status)////显示提示框，目前三个参数(txt：要显示的文本；time：自动关闭的时间（不设置的话默认1500毫秒）；status：默认0为错误提示，1为正确提示；)
{
    var htmlCon = '';
    if(txt != ''){
        if(status != 0 && status != undefined){
            htmlCon = '<div class="tipsBox" style="width:220px;padding:10px;background-color:#4AAF33;border-radius:4px;-webkit-border-radius: 4px;-moz-border-radius: 4px;color:#fff;box-shadow:0 0 3px #ddd inset;-webkit-box-shadow: 0 0 3px #ddd inset;text-align:center;position:fixed;top:25%;left:50%;z-index:999999;margin-left:-120px;"><img src="images/ok.png" style="vertical-align: middle;margin-right:5px;" alt="OK，"/>'+txt+'</div>';
        }else{
            htmlCon = '<div class="tipsBox" style="width:220px;padding:10px;background-color:#D84C31;border-radius:4px;-webkit-border-radius: 4px;-moz-border-radius: 4px;color:#fff;box-shadow:0 0 3px #ddd inset;-webkit-box-shadow: 0 0 3px #ddd inset;text-align:center;position:fixed;top:25%;left:50%;z-index:999999;margin-left:-120px;"><img src="images/err.png" style="vertical-align: middle;margin-right:5px;" alt="Error，"/>'+txt+'</div>';
        }
        $('body').prepend(htmlCon);
        if(time == '' || time == undefined){
            time = 2500;
        }
        setTimeout(function(){ $('.tipsBox').remove(); },time);
    }
}

// 索引查询
function queryIndex(){
    $("#es_url").validate2("连接es的ip和端口");
    $("#esCluster_name").validate2("es集群名称");
    var es_url =  $("#es_url").val();
    var esCluster_name = $("#esCluster_name").val();
    $.ajax({
        type: "POST",
        data: {
            "es_url": es_url,
            "esCluster_name":esCluster_name
        },
        url: 'index',
        dataType: 'json',
        cache: false,
        async: false,
        success: function (result) {
            if (result.succeed){
                var $dialogContent = $('<div style="height: 350px; overflow: auto;"></div>');

                //$dialogContent.append(
                //    '<table id="tab" border="1" cellpadding="10">'+
                //    '<thead> <tr><th width="200px;">索引名称</th><th width="200px;">类型</th></tr> </thead>'+
                //    '<tbody>'
                //);
                var dd = [];
                dd.push( '<table id="tab" border="1" cellpadding="10">'+
                    '<thead> <tr><th width="200px;">索引名称</th><th width="200px;">类型</th></tr> </thead>'+
                    '<tbody>');
                for (var i = 0; i < result.indices.length; i++){
                    dd.push('<tr><td>'+result.indices[i].index_name+'</td><td>'+result.indices[i].index_type+'</td></tr>');
                    if (i==result.indices.length-1){
                        dd.push('</tbody> </table>');
                    }
                }
                //alert(dd.join(''));
                $dialogContent.append(dd.join(''));
                var dialog = BootstrapDialog.show({
                    title: '索引列表',
                    message: $dialogContent,
                    type:  'type-primary',
                    cssClass: 'login-dialog',
                    draggable: true,
                    size: 'size-normal',
                    onshow: function(dialogRef){
                        // alert(dialogRef.getMessage());
                    },
                });
            }else {
                alert(result.msg);
            }
        },
        error: function (error) {
            //$("#returndata").html("出现未知错误！请查看schema格式是否正确！");
            alert("出现未知错误！请查看输入信息是否正确以及集群是否启动！");
        }
    });



}