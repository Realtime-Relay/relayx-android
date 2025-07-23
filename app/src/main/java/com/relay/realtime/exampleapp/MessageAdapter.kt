package com.relay.realtime.exampleapp

import android.view.LayoutInflater
import android.view.View
import android.view.ViewGroup
import android.widget.TextView
import androidx.recyclerview.widget.RecyclerView
import com.relay.realtime.R

class MessageAdapter(private val items: List<MessageItem>) : RecyclerView.Adapter<MessageAdapter.Holder>() {

    class Holder(v: View) : RecyclerView.ViewHolder(v) {
        val from: TextView = v.findViewById(R.id.fromTxt)
        val body: TextView = v.findViewById(R.id.bodyTxt)
    }

    override fun onCreateViewHolder(parent: ViewGroup, viewType: Int): Holder {
        val v = LayoutInflater.from(parent.context).inflate(R.layout.item_message, parent, false)
        return Holder(v)
    }

    override fun onBindViewHolder(h: Holder, position: Int) {
        val m = items[position]
        h.from.text = m.from
        h.body.text = m.body
    }

    override fun getItemCount(): Int = items.size
}